from dlstbx.ispybtbx import ispybtbx
import argparse
from pprint import pprint
import json
from datetime import datetime


def read_data_from_ispyb(jobids=None, dtstamp_start=None, dtstamp_end=None):

    ispyb_conn = ispybtbx()

    str_jobids = []
    if jobids:
        if len(jobids) == 1:
            str_jobids.append(f"AND pj.processingJobId = {jobids[0]}")
        else:
            str_jobids.append(f"AND pj.processingJobId IN {tuple(jobids)}")
        print(f"Reading data for following ep_predict jobids: {jobids}")
    if dtstamp_start:
        str_jobids.append(f"AND pj.recordTimestamp > '{dtstamp_start}'")
    if dtstamp_end:
        str_jobids.append(f"AND pj.recordTimestamp < '{dtstamp_end}'")
    str_jobids = " ".join(str_jobids)

    sql_str = f"""
SELECT DISTINCT
    pj.processingJobId as rpid,
    app.autoprocprogramid as program_id,
    pj.dataCollectionId as dc_id,
    app.processingCommandLine as name,
    appa.filePath as filepath,
    pjdown.processingJobId as bigep_jobid,
    CONCAT(appadown.filePath, "/", appadown.fileName) as bigep_json,
    CONCAT(appaep.filePath, "/", appaep.fileName) as ep_predict_json,
    pj.recordTimestamp AS datetime_stamp
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
        pj2.processingjobid > 3700000) pjup ON
    pjup.datacollectionid = pj.dataCollectionId
INNER JOIN AutoProcProgram app ON
    app.processingJobId = pjup.processingJobId
INNER JOIN AutoProcProgramAttachment appa ON
    appa.autoProcProgramId = app.autoProcProgramId
INNER JOIN (
    SELECT
        pj2.processingJobId,
        pj2.datacollectionid,
        pj2.recipe
    FROM
        ProcessingJob pj2
    WHERE
        pj2.processingjobid > 3700000) pjdown ON
    pjdown.datacollectionid = pj.dataCollectionId
INNER JOIN ProcessingJobParameter pjpdown ON
    pjpdown.processingJobId = pjdown.processingJobId
INNER JOIN AutoProcProgram appdown ON
    appdown.processingJobId = pjdown.processingJobId
INNER JOIN AutoProcProgramAttachment appadown ON
    appadown.autoProcProgramId = appdown.autoProcProgramId
INNER JOIN AutoProcProgram appep ON
    appep.processingJobId = pj.processingJobId
INNER JOIN AutoProcProgramAttachment appaep ON
    appaep.autoProcProgramId = appep.autoProcProgramId
WHERE
    pjup.processingjobid <> pj.processingjobid
    AND pjp.parameterKey = 'data'
    AND INSTR(pjp.parameterValue, appa.filePath) = 1
    AND pjpdown.parameterKey = "program_id"
    AND pjpdown.parameterValue = app.autoprocprogramid
    AND pjdown.recipe = "postprocessing-big-ep-launcher"
    AND appadown.fileName = "big_ep_model_ispyb.json"
    AND pj.processingjobid > 3700000
    AND pj.displayName = 'ep_predict'
    {str_jobids}
"""

    columns = (
        "rpid",
        "program_id",
        "dc_id",
        "pipeline",
        "filepath",
        "bigep_jobid",
        "bigep_json",
        "ep_predict_json",
        "datetime_stamp",
    )
    rows = ispyb_conn.execute(sql_str)
    results = [dict(zip(columns, rec)) for rec in rows]
    print(f"Found {len(rows)} relevant records in ISPyB")
    return results[:]


def read_bigep_results(rows):
    results = {}
    for row in rows:
        rpid = row["rpid"]
        if rpid not in results:
            results[rpid] = {}
        try:
            with open(row["bigep_json"]) as fp:
                res = json.load(fp)
        except Exception:
            print(
                f"Cannot read big_ep results file {row['bigep_json']} for jobid {rpid}"
            )
            continue
        try:
            results[rpid][res["pipeline"]] = res
        except KeyError:
            if "autoSHARP" in row["bigep_json"]:
                ppl = "autoSHARP"
            elif "AutoSol" in row["bigep_json"]:
                ppl = "AutoBuild"
            elif "crank2" in row["bigep_json"]:
                ppl = "Crank2"
            else:
                raise ValueError(
                    f"Unidentified model building pipeline for {row['bigep_json']}"
                )
            results[rpid][ppl] = res
        try:
            with open(row["ep_predict_json"]) as fp:
                res = json.load(fp)
        except Exception:
            print(
                f"Cannot read ep_predict results file {row['ep_predict_json']} for jobid {rpid}"
            )
            continue
        results[rpid]["ep_predict"] = res
        results[rpid]["ep_predict"]["datetime_stamp"] = row[
            "datetime_stamp"
        ].isoformat()
    return results


def write_results(res, filename):
    if filename:
        with open(filename, "w") as fp:
            json.dump(res, fp, indent=4, sort_keys=True)
    else:
        pprint(res)


def run(jobids, dtstamp_start, dtstamp_end, json_file):
    rows = read_data_from_ispyb(jobids, dtstamp_start, dtstamp_end)
    results = read_bigep_results(rows)
    write_results(results, json_file)


def runmain():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="ep_predict_results - Read experimental phasing predictor results"
        " and related downstream processing task output",
        epilog="For the given list of ep_predict jobids read big_ep json files with model building results"
        "\nData is printed in the terminal or written into json file if file name is provided."
        "\nResults contain summary of characteristics of build big_ep models"
        "\nwith corresponding ep_predictor task output.",
    )
    parser.add_argument("jobids", help="List of ep_predict jobids", nargs="*", type=int)
    parser.add_argument("-j", "--json", help="Json file name for output results")
    parser.add_argument(
        "-s",
        "--start_date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="List results from the given start date",
    )
    parser.add_argument(
        "-e",
        "--end_date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="List results until the given end date",
    )

    args = parser.parse_args()
    run(args.jobids, args.start_date, args.end_date, args.json)
