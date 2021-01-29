import argparse
from pprint import pprint
import json
from pathlib import Path

from dlstbx.ispybtbx import ispybtbx
from dlstbx.util import mr_utils
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
    pj.processingJobId AS rpid,
    dc.dataCollectionId AS dc_id,
    appa.filePath as filepath,
    appa.fileName AS mr_predict_json,
    pjp.parameterValue AS mrbump_logfile,
    pj.recordTimestamp AS datetime_stamp
FROM
    ProcessingJob pj
INNER JOIN AutoProcProgram app ON
    app.processingJobId = pj.processingJobId
INNER JOIN ProcessingJobParameter pjp ON pjp.processingJobId = pj.processingJobId
INNER JOIN AutoProcProgramAttachment appa ON
    appa.autoProcProgramId = app.autoProcProgramId
INNER JOIN DataCollection dc ON
    dc.dataCollectionId = pj.dataCollectionId
WHERE
    appa.fileType = "Result"
    AND pjp.parameterValue LIKE "%MRBUMP.log"
    AND pj.processingjobid > 3700000
    AND pj.displayName = 'mr_predict'
    {str_jobids}
"""

    columns = (
        "rpid",
        "dc_id",
        "filepath",
        "mr_predict_json",
        "mrbump_logfile",
        "datetime_stamp",
    )
    rows = ispyb_conn.execute(sql_str)
    results = [dict(zip(columns, rec)) for rec in rows]
    print(f"Found {len(rows)} relevant records in ISPyB")
    return results[:]


def read_mr_results(rows):
    results = {}
    for row in rows:
        rpid = row["rpid"]
        if rpid not in results:
            results[rpid] = {
                "MrBUMP": mr_utils.get_mrbump_metrics(row["mrbump_logfile"])
            }
        json_logfile = Path(row["filepath"]) / row["mr_predict_json"]
        try:
            results[rpid]["mr_predict"] = json.loads(json_logfile.read_text())
            results[rpid]["mr_predict"]["datetime_stamp"] = row[
                "datetime_stamp"
            ].isoformat()
        except Exception:
            print(f"Cannot read results for jobid {rpid}")
            continue
    return results


def write_results(res, filename):
    if filename:
        with open(filename, "w") as fp:
            json.dump(res, fp, indent=4, sort_keys=True)
    else:
        pprint(res)


def run(jobids, start_date, end_date, json_file):
    rows = read_data_from_ispyb(jobids, start_date, end_date)
    results = read_mr_results(rows)
    write_results(results, json_file)


def runmain():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="mr_predict_results - Read molecular replacement predictor results"
        " and related MrBUMP processing task output",
        epilog="For the given list of mr_predict jobids read json results files."
        "\nData is printed in the terminal or written into json file if file name is provided."
        "\nResults contain summary of characteristics of build MrBUMP models"
        "\nwith corresponding mr_predictor task output.",
    )
    parser.add_argument("jobids", help="List of mr_predict jobids", nargs="*", type=int)
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
