import argparse
from pprint import pprint
import json
from pathlib import Path

from dlstbx.ispybtbx import ispybtbx
from dlstbx.util import mr_utils


def read_data_from_ispyb(jobids):

    ispyb_conn = ispybtbx()

    if len(jobids) == 1:
        str_jobids = f"= {jobids[0]}"
    else:
        str_jobids = f"IN {tuple(jobids)}"
    print(f"Reading data for following mr_predict jobids: {jobids}")

    sql_str = f"""
SELECT DISTINCT
    pj.processingJobId AS rpid,
    dc.dataCollectionId AS dc_id,
    appa.filePath as filepath,
    appa.fileName AS mr_predict_json,
    pjp.parameterValue AS mrbump_logfile
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
    AND pj.processingJobId {str_jobids}
"""

    columns = (
        "rpid",
        "dc_id",
        "filepath",
        "mr_predict_json",
        "mrbump_logfile",
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
        results[rpid]["mr_predict"] = json.loads(json_logfile.read_text())
    return results


def write_results(res, filename):
    if filename:
        with open(filename, "w") as fp:
            json.dump(res, fp, indent=4, sort_keys=True)
    else:
        pprint(res)


def run(jobids, json_file):
    rows = read_data_from_ispyb(jobids)
    results = read_mr_results(rows)
    write_results(results, json_file)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="mr_predict_results - Read molecular replacement predictor results"
        " and related MrBUMP processing task output",
        epilog="For the given list of mr_predict jobids read json results files."
        "\nData is printed in the terminal or written into json file if file name is provided."
        "\nResults contain summary of characteristics of build MrBUMP models"
        "\nwith corresponding mr_predictor task output.",
    )
    parser.add_argument("jobids", help="List of mr_predict jobids", nargs="+", type=int)
    parser.add_argument("-j", "--json", help="Json file name for output results")

    args = parser.parse_args()
    run(args.jobids, args.json)
