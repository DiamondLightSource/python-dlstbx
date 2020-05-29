from dlstbx.ispybtbx import ispybtbx
import os.path
from pprint import pprint
import procrunner
from time import sleep
import argparse


def read_ispyb_data(jobids):
    ispyb_conn = ispybtbx()

    if len(jobids) == 1:
        str_jobids = f"= {jobids[0]}"
    else:
        str_jobids = f"IN {tuple(jobids)}"
    print(f"Reading data for following ep_predict jobids: {jobids}")

    sql_str = f"""
SELECT DISTINCT
    pj.processingJobId as rpid,
    app.autoprocprogramid as program_id,
    pj.dataCollectionId as dc_id,
    app.processingCommandLine as name,
    appa.filePath as filepath
FROM
    ProcessingJob pj
INNER JOIN ProcessingJobParameter pjp ON
    pj.processingJobId = pjp.processingJobId
INNER JOIN (
    SELECT
        pj2.processingJobId,
        pj2.datacollectionid
    FROM
        ProcessingJob pj2
    WHERE
        pj2.processingjobid > 3700000) pjup on
    pjup.datacollectionid = pj.dataCollectionId
INNER JOIN AutoProcProgram app ON
    app.processingJobId = pjup.processingJobId
INNER JOIN AutoProcProgramAttachment appa ON
    appa.autoProcProgramId = app.autoProcProgramId
WHERE
    pjup.processingjobid <> pj.processingjobid
    AND pjp.parameterKey = 'data'
    AND INSTR(pjp.parameterValue, appa.filePath) = 1
    AND pj.processingJobId {str_jobids}"""

    columns = ("rpid", "program_id", "dc_id", "pipeline", "filepath")

    results = [dict(zip(columns, rec)) for rec in ispyb_conn.execute(sql_str)]
    pprint(results)
    return results[:]


def run_ispyb_job(data, debug):
    for v in data:
        filename = os.path.join(
            v["filepath"], "DataFiles", "AUTOMATIC_DEFAULT_free.mtz"
        )
        if os.path.isfile(filename):
            command = [
                "ispyb.job",
                "--new",
                "--dcid",
                str(v["dc_id"]),
                "--recipe",
                "postprocessing-big-ep-launcher",
                "--add-param",
                "program_id:{}".format(v["program_id"]),
                "--add-param",
                "data:{}".format(filename),
            ]
            print(" ".join(command))

            if debug:
                return

            result = procrunner.run(command, timeout=100, working_directory="/tmp",)
            if not result["stdout"]:
                print("No output written by ispyb.job")
            job_file = "/tmp/stdout_{}.dat".format(v["rpid"])
            stdout_data = result["stdout"].decode("latin1")
            print(stdout_data)
            with open(job_file, "w") as fp:
                fp.write(stdout_data)
        else:
            print(f"File {filename} not found")


def trigger_dlstbx_go(data, arg_sleep, debug):

    programids = tuple((v["program_id"] for v in data))
    if not programids:
        raise ValueError("program_id values not found")

    print(f"Running jobs with following pragram_id values: {programids}")
    if len(programids) == 1:
        str_programids = f"= {programids[0]}"
    else:
        str_programids = f"IN {programids}"

    ispyb_conn = ispybtbx()

    sql_str = f"""
SELECT
    pjp.processingJobId as rpid,
    pjp.parameterValue as program_id
FROM
    ProcessingJobParameter pjp
INNER JOIN ProcessingJob pj ON
    pj.processingJobId = pjp.processingJobId
WHERE
    pjp.parameterKey = "program_id"
    AND pjp.parameterValue {str_programids}
    AND pj.recipe = "postprocessing-big-ep-launcher"
ORDER BY pjp.processingJobId DESC LIMIT {len(programids)}
 """

    columns = ("rpid", "program_id")

    results = [dict(zip(columns, rec)) for rec in ispyb_conn.execute(sql_str)]
    if not results:
        raise ValueError(
            f"No processingJob records found matching program_id values {programids}"
        )

    for v in results:
        try:
            filepath = next(
                (
                    d["filepath"]
                    for d in data
                    if int(d["program_id"]) == int(v["program_id"])
                )
            )
        except StopIteration:
            print("Cannot find data for program_id {}".format(v["program_id"]))
            continue
        if "multi-xia2-3dii" in filepath:
            path_ext = "multi-xia2/3dii"
        elif "multi-xia2-dials" in filepath:
            path_ext = "multi-xia2/dials"
        elif "3dii" in filepath:
            path_ext = "xia2/3dii-run"
        elif "dials" in filepath:
            path_ext = "xia2/dials-run"
        else:
            print("Unrecognised file path %s" % filepath)
            continue
        command = [
            "dlstbx.go",
            "-p",
            str(v["rpid"]),
            "-s",
            "path_ext={}".format(path_ext),
            "-v",
        ]
        print(" ".join(command))

        if debug:
            return

        result = procrunner.run(command, timeout=100, working_directory="/tmp",)
        if not result["stdout"]:
            print("No output written by dlstbx.go")
        job_file = "/tmp/dlstbx_go_{}.dat".format(v["rpid"])
        stdout_data = result["stdout"].decode("latin1")
        print(stdout_data)
        with open(job_file, "w") as fp:
            fp.write(stdout_data)
        sleep(arg_sleep)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="ep_predict_phase - Run big_ep phasing jobs on data from ep_predict results",
        epilog="For the given list of ep_predict jobids run big_ep phasing on the corresponding xia2 data.",
    )
    parser.add_argument("jobids", help="List of ep_predict jobids", nargs="+", type=int)
    parser.add_argument(
        "--dry-run", help="Do not create database records", action="store_true"
    )
    parser.add_argument(
        "-s",
        "--sleep",
        help="Time in seconds between triggering of successive jobs (Default: 1200s)",
        type=int,
        default=1200,
    )

    args = parser.parse_args()

    data = read_ispyb_data(args.jobids)
    run_ispyb_job(data, args.dry_run)
    trigger_dlstbx_go(data, args.sleep, args.dry_run)
