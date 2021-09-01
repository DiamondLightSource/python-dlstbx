import argparse
import json
from datetime import datetime
from pathlib import Path
from pprint import pprint

import ispyb
import ispyb.sqlalchemy
import sqlalchemy
from ispyb.sqlalchemy import (
    AutoProcProgram,
    AutoProcProgramAttachment,
    DataCollection,
    ProcessingJob,
    ProcessingJobParameter,
)

from dlstbx.util import mr_utils


def read_data_from_ispyb(jobids=None, dtstamp_start=None, dtstamp_end=None):

    url = ispyb.sqlalchemy.url()
    engine = sqlalchemy.create_engine(url, connect_args={"use_pure": True})
    db_session_maker = sqlalchemy.orm.sessionmaker(bind=engine)

    with db_session_maker() as db_session:
        filter_list = [
            AutoProcProgramAttachment.fileType == "Result",
            ProcessingJobParameter.parameterValue.like("%MRBUMP.log"),
            ProcessingJob.processingJobId > 3700000,
            ProcessingJob.displayName == "mr_predict",
        ]
        if jobids:
            if len(jobids) == 1:
                filter_list.append(ProcessingJob.processingJobId == int(jobids[0]))
            else:
                filter_list.append(
                    ProcessingJob.processingJobId.in_(tuple(int(jid) for jid in jobids))
                )
            print(f"Reading data for following ep_predict jobids: {jobids}")
        if dtstamp_start:
            filter_list.append(ProcessingJob.recordTimestamp > dtstamp_start)
        if dtstamp_end:
            filter_list.append(ProcessingJob.recordTimestamp < dtstamp_end)
        query = (
            db_session.query(
                ProcessingJob.processingJobId.label("rpid"),
                DataCollection.dataCollectionId.label("dc_id"),
                AutoProcProgramAttachment.filePath.label("filepath"),
                AutoProcProgramAttachment.fileName.label("mr_predict_json"),
                ProcessingJobParameter.parameterValue.label("mrbump_logfile"),
                ProcessingJob.recordTimestamp.label("datetime_stamp"),
            )
            .join(
                AutoProcProgram,
                AutoProcProgram.processingJobId == ProcessingJob.processingJobId,
            )
            .join(
                ProcessingJobParameter,
                ProcessingJobParameter.processingJobId == ProcessingJob.processingJobId,
            )
            .join(
                AutoProcProgramAttachment,
                AutoProcProgramAttachment.autoProcProgramId
                == AutoProcProgram.autoProcProgramId,
            )
            .join(
                DataCollection,
                DataCollection.dataCollectionId == ProcessingJob.dataCollectionId,
            )
            .filter(*filter_list)
        )
        rows = list(query.distinct().all())

    results = [dict(zip(row.keys(), row)) for row in rows]
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
