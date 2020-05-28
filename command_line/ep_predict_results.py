from dlstbx.ispybtbx import ispybtbx
import argparse
from pprint import pprint
import json


def read_data_from_ispyb(jobids):

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
    pjdown.processingJobId as bigep_jobid,
    CONCAT(appadown.filePath, "/", appadown.fileName) as bigep_json,
    CONCAT(appaep.filePath, "/", appaep.fileName) as ep_predict_json
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
    AND pj.processingJobId {str_jobids}
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
        with open(row["bigep_json"]) as fp:
            res = json.load(fp)
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
                    raise ValueError("Unidentified model building pipeline")
                results[rpid][ppl] = res
        with open(row["ep_predict_json"]) as fp:
            res = json.load(fp)
            results[rpid]["ep_predict"] = res

    return results


def write_results(res, filename):
    if filename:
        with open(filename, "w") as fp:
            json.dump(res, fp, indent=4, sort_keys=True)
    else:
        pprint(res)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="ep_predict_results - Read experimental phasing predictor results"
        " and related downstream processing task output",
        epilog="For the given list of ep_predict jobids read big_ep json files with model building results"
        "\nData is printed in the terminal or written into json file if file name is provided."
        "\nResults contain summary of characteristics of build big_ep models"
        "\nwith corresponding ep_predictor task output.",
    )
    parser.add_argument("jobids", help="List of ep_predict jobids", nargs="+", type=int)
    parser.add_argument("-j", "--json", help="Json file name for output results")

    args = parser.parse_args()

    rows = read_data_from_ispyb(args.jobids)
    results = read_bigep_results(rows)
    write_results(results, args.json)
