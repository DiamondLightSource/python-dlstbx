import argparse
import json
from datetime import datetime
from pprint import pprint

import ispyb.sqlalchemy
import sqlalchemy.orm
from ispyb.sqlalchemy import (
    AutoProcProgram,
    AutoProcProgramAttachment,
    ProcessingJob,
    ProcessingJobParameter,
)
from sqlalchemy import or_


def read_data_from_ispyb(jobids=None, dtstamp_start=None, dtstamp_end=None):

    url = ispyb.sqlalchemy.url()
    engine = sqlalchemy.create_engine(url, connect_args={"use_pure": True})
    db_session_maker = sqlalchemy.orm.sessionmaker(bind=engine)

    with db_session_maker() as db_session:
        subquery_up = (
            db_session.query(
                ProcessingJob.processingJobId,
                ProcessingJob.dataCollectionId,
                AutoProcProgram.autoProcProgramId,
                AutoProcProgram.processingCommandLine,
                AutoProcProgramAttachment.filePath,
            )
            .join(
                AutoProcProgram,
                AutoProcProgram.processingJobId == ProcessingJob.processingJobId,
            )
            .join(
                AutoProcProgramAttachment,
                AutoProcProgramAttachment.autoProcProgramId
                == AutoProcProgram.autoProcProgramId,
            )
            .filter(ProcessingJob.processingJobId > 3700000)
        ).subquery()

        subquery_down = (
            db_session.query(
                ProcessingJob.processingJobId,
                ProcessingJob.dataCollectionId,
                ProcessingJobParameter.parameterValue,
                AutoProcProgramAttachment.filePath,
                AutoProcProgramAttachment.fileName,
            )
            .join(
                ProcessingJobParameter,
                ProcessingJobParameter.processingJobId == ProcessingJob.processingJobId,
            )
            .join(
                AutoProcProgram,
                AutoProcProgram.processingJobId == ProcessingJob.processingJobId,
            )
            .join(
                AutoProcProgramAttachment,
                AutoProcProgramAttachment.autoProcProgramId
                == AutoProcProgram.autoProcProgramId,
            )
            .filter(ProcessingJob.processingJobId > 3700000)
            .filter(ProcessingJob.recipe == "postprocessing-big-ep-launcher")
            .filter(ProcessingJobParameter.parameterKey == "program_id")
            .filter(AutoProcProgramAttachment.fileName == "big_ep_model_ispyb.json")
        ).subquery()

        query = (
            db_session.query(
                ProcessingJob.processingJobId.label("rpid"),
                AutoProcProgram.autoProcProgramId.label("program_id"),
                ProcessingJob.dataCollectionId.label("dc_id"),
                subquery_up.c.processingCommandLine.label("name"),
                subquery_up.c.filePath.label("filepath"),
                subquery_down.c.processingJobId.label("bigep_jobid"),
                sqlalchemy.func.concat(
                    subquery_down.c.filePath, "/", subquery_down.c.fileName
                ).label("bigep_json"),
                sqlalchemy.func.concat(
                    AutoProcProgramAttachment.filePath,
                    "/",
                    AutoProcProgramAttachment.fileName,
                ).label("ep_predict_json"),
                ProcessingJob.recordTimestamp.label("datetime_stamp"),
            )
            .join(
                ProcessingJobParameter,
                ProcessingJobParameter.processingJobId == ProcessingJob.processingJobId,
            )
            .join(
                AutoProcProgram,
                AutoProcProgram.processingJobId == ProcessingJob.processingJobId,
            )
            .join(
                AutoProcProgramAttachment,
                AutoProcProgramAttachment.autoProcProgramId
                == AutoProcProgram.autoProcProgramId,
            )
            .join(
                subquery_up,
                subquery_up.c.dataCollectionId == ProcessingJob.dataCollectionId,
            )
            .outerjoin(
                subquery_down,
                subquery_down.c.dataCollectionId == ProcessingJob.dataCollectionId,
            )
            .filter(subquery_up.c.processingJobId != ProcessingJob.processingJobId)
            .filter(ProcessingJobParameter.parameterKey == "program_id")
            .filter(
                ProcessingJobParameter.parameterValue == subquery_up.c.autoProcProgramId
            )
            .filter(
                or_(
                    subquery_down.c.parameterValue == subquery_up.c.autoProcProgramId,
                    subquery_down.c.parameterValue.is_(None),
                )
            )
            .filter(ProcessingJob.processingJobId > 3700000)
            .filter(ProcessingJob.displayName == "ep_predict")
        )
        if jobids:
            if len(jobids) == 1:
                query = query.filter(ProcessingJob.processingJobId == int(jobids[0]))
            else:
                query = query.filter(
                    ProcessingJob.processingJobId.in_(tuple(int(jid) for jid in jobids))
                )
            print(f"Reading data for following ep_predict jobids: {jobids}")
        if dtstamp_start:
            query = query.filter(ProcessingJob.recordTimestamp > dtstamp_start)
        if dtstamp_end:
            query = query.filter(ProcessingJob.recordTimestamp < dtstamp_end)
        rows = list(query.distinct().all())

    results = [dict(zip(row.keys(), row)) for row in rows]
    print(f"Found {len(results)} ep_predict records in ISPyB")
    big_ep_results = [res for res in results if res["bigep_jobid"] is not None]
    print(f"Found {len(big_ep_results)} BigEP results in ISPyB")
    return results[:]


def read_bigep_results(rows):
    results = {}
    for row in rows:

        rpid = row["rpid"]
        if rpid not in results:
            results[rpid] = {
                k: row[k]
                for k in ("dc_id", "program_id", "name", "filepath", "bigep_jobid")
            }

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

        if row["bigep_json"]:
            try:
                with open(row["bigep_json"]) as fp:
                    res = json.load(fp)
            except Exception:
                print(
                    f"Cannot read big_ep results for {row['ep_predict_json']} jobid {rpid}"
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
