from dlstbx.ispybtbx import ispybtbx
from pprint import pprint
import procrunner
from time import sleep
import argparse
from pathlib import Path
import re


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
    appa.filePath as filepath,
    appa.fileName as filename
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
    AND pjp.parameterKey = 'program_id'
    AND pjp.parameterValue = appa.autoProcProgramId
    AND appa.fileType = "Result"
    AND (appa.fileName LIKE "%free.mtz"
         OR appa.filename LIKE "%unique.mtz")
    AND pj.processingJobId {str_jobids}"""

    columns = ("program_id", "dc_id", "pipeline", "filepath", "filename")

    results = {
        rec[0]: dict(zip(columns, rec[1:])) for rec in ispyb_conn.execute(sql_str)
    }
    return results


def read_big_ep_jobids(data, last_records=True):

    programids = {data[v]["program_id"]: v for v in data}
    if not programids:
        raise ValueError("program_id values not found")
    if len(programids) == 1:
        str_programids = f"= {tuple(programids)[0]}"
    else:
        str_programids = f"IN {tuple(programids)}"
    if last_records:
        str_last_records = f"DESC LIMIT {len(programids)}"
    else:
        str_last_records = ""

    ispyb_conn = ispybtbx()

    sql_str = f"""
SELECT
    pjp.processingJobId as rpid,
    CAST(pjp.parameterValue AS UNSIGNED) as program_id
FROM
    ProcessingJobParameter pjp
INNER JOIN ProcessingJob pj ON
    pj.processingJobId = pjp.processingJobId
WHERE
    pjp.parameterKey = "program_id"
    AND pjp.parameterValue {str_programids}
    AND (pj.recipe = "postprocessing-big-ep"
         OR pj.recipe = "postprocessing-big-ep-setup")
ORDER BY pjp.processingJobId {str_last_records}
 """

    columns = ("rpid", "program_id")
    rows = ispyb_conn.execute(sql_str)
    results = [dict(zip(columns, row)) for row in rows]
    results = {programids[res["program_id"]]: res for res in results}
    if results:
        print(
            f"\nList of upstream AutoProcProgramId values for predictor input data: {list(programids)}"
        )
        print("Downstream BigEP jobid values:")
        pprint(results)
    return results


def run_ispyb_job(data, debug, dry_run):
    for _, v in data.items():
        filename = Path(v["filepath"]) / v["filename"]
        if not filename.is_file():
            print(f"File {filename} not found. Skipping.")
            continue
        visit_match = re.search(r"/([a-z]{2}[0-9]{4,5}-[0-9]+)/", v["filepath"])
        try:
            visit = visit_match.group(1)
        except AttributeError:
            print(f"Cannot match visit pattern in path {str(filename)}. Skipping")
            continue
        if True in [pfx in visit for pfx in ("lb", "in", "sw")]:
            print(
                f"Skipping processing for data from an industrial visit {visit}: {str(filename)}"
            )
            continue
        command = [
            "ispyb.job",
            "--new",
            "--dcid",
            str(v["dc_id"]),
            "--display",
            "big_ep",
            "--comment",
            "big_ep via ep_predict_phase",
            "--recipe",
            "postprocessing-big-ep-setup",
            "--add-param",
            f"program_id:{v['program_id']}",
            "--add-param",
            f"data:{filename}",
        ]
        print(f"\nRegister BigEP job: {' '.join(command)}")

        if not dry_run:
            result = procrunner.run(
                command,
                timeout=100,
                raise_timeout_exception=True,
                print_stdout=debug,
                working_directory="/tmp",
            )
            if not result.stdout:
                print("WARNING: No output written by ispyb.job")


def trigger_dlstbx_go(data, arg_sleep, debug, dry_run):

    results = read_big_ep_jobids(data)
    if not results:
        raise ValueError(
            f"No processingJob records found matching jobid values {list(data)}"
        )

    for i, (jobid, v) in enumerate(results.items(), 1):
        try:
            filepath = data[jobid]["filepath"]
        except StopIteration:
            print(f"Cannot find data for jobid {jobid}")
            continue
        if "multi-xia2-3dii" in filepath:
            path_ext = "multi-xia2/3dii"
        elif "multi-xia2-dials" in filepath:
            path_ext = "multi-xia2/dials"
        elif "3dii" in filepath:
            path_ext = "xia2/3dii-run"
        elif "dials" in filepath:
            path_ext = "xia2/dials-run"
        elif "truncate-unique" in filepath:
            path_ext = "autoPROC/ap-run"
        elif "staraniso" in filepath:
            path_ext = "autoPROC-STARANISO/ap-run"
        else:
            print("Unrecognised file path %s" % filepath)
            continue
        command = [
            "dlstbx.go",
            "--test",
            "-s",
            f"path_ext={path_ext}",
            "-p",
            str(v["rpid"]),
        ]
        print(f"\nTrigger BigEP job: {' '.join(command)}")

        if dry_run:
            continue

        result = procrunner.run(
            command,
            timeout=100,
            raise_timeout_exception=True,
            print_stdout=debug,
            working_directory="/tmp",
        )
        if not result.stdout:
            print("WARNING: No output written by dlstbx.go")
        if i < len(results):
            sleep(arg_sleep)


if __name__ == "__main__":

    def __timeout(arg, max_val=600):
        try:
            val = int(arg)
        except ValueError:
            raise argparse.ArgumentTypeError("must be an integer")
        if val < max_val:
            raise argparse.ArgumentTypeError(f"must be at least {max_val}s.")
        return val

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
        "--debug",
        help="Print Zocalo output messages to the console",
        action="store_true",
    )
    parser.add_argument(
        "-s",
        "--sleep",
        help="Time in seconds between triggering of successive jobs (Default: 1200s)",
        type=__timeout,
        default=1200,
    )

    args = parser.parse_args()

    data = read_ispyb_data(args.jobids)
    bigep_jobids = read_big_ep_jobids(data, False)
    if bigep_jobids:
        print(f"WARNING: Found records of the existing big_ep runs {bigep_jobids}")
        print("         New results might not be visible in SynchWeb.")
    run_ispyb_job(data, args.debug, args.dry_run)
    trigger_dlstbx_go(data, args.sleep, args.debug, args.dry_run)
