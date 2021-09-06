import argparse
import re
from pathlib import Path
from pprint import pprint
from time import sleep

import ispyb.sqlalchemy
import procrunner
import sqlalchemy.orm
from ispyb.sqlalchemy import (
    AutoProcProgram,
    AutoProcProgramAttachment,
    ProcessingJob,
    ProcessingJobParameter,
)
from sqlalchemy import Integer, cast, desc, or_


def read_ispyb_data(jobids):
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
                AutoProcProgramAttachment.fileName,
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
            .filter(AutoProcProgramAttachment.fileType == "Result")
            .filter(
                or_(
                    AutoProcProgramAttachment.fileName.contains("free.mtz"),
                    AutoProcProgramAttachment.fileName.contains("unique.mtz"),
                    AutoProcProgramAttachment.fileName == "scaled.mtz",
                )
            )
        ).subquery()

        query = (
            db_session.query(
                ProcessingJob.processingJobId.label("rpid"),
                subquery_up.c.autoProcProgramId.label("program_id"),
                ProcessingJob.dataCollectionId.label("dc_id"),
                subquery_up.c.processingCommandLine.label("pipeline"),
                subquery_up.c.filePath.label("filepath"),
                subquery_up.c.fileName.label("filename"),
            )
            .join(
                ProcessingJobParameter,
                ProcessingJobParameter.processingJobId == ProcessingJob.processingJobId,
            )
            .join(
                subquery_up,
                subquery_up.c.dataCollectionId == ProcessingJob.dataCollectionId,
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
            .filter(subquery_up.c.processingJobId != ProcessingJob.processingJobId)
            .filter(ProcessingJobParameter.parameterKey == "program_id")
            .filter(
                ProcessingJobParameter.parameterValue == subquery_up.c.autoProcProgramId
            )
            .filter(ProcessingJob.processingJobId > 3700000)
        )
        if jobids:
            if len(jobids) == 1:
                query = query.filter(ProcessingJob.processingJobId == int(jobids[0]))
            else:
                query = query.filter(
                    ProcessingJob.processingJobId.in_(tuple(int(jid) for jid in jobids))
                )
            print(f"Reading data for following ep_predict jobids: {jobids}")
        rows = list(query.distinct().all())

    results = {row["rpid"]: dict(list(zip(row.keys(), row))[1:]) for row in rows}
    print(f"Found {len(rows)} relevant records in ISPyB")
    pprint(results)
    return results


def read_big_ep_jobids(data, last_records=True):

    url = ispyb.sqlalchemy.url()
    engine = sqlalchemy.create_engine(url, connect_args={"use_pure": True})
    db_session_maker = sqlalchemy.orm.sessionmaker(bind=engine)

    with db_session_maker() as db_session:
        query = (
            db_session.query(
                ProcessingJobParameter.processingJobId.label("rpid"),
                cast(ProcessingJobParameter.parameterValue, Integer).label(
                    "program_id"
                ),
            )
            .join(
                ProcessingJob,
                ProcessingJob.processingJobId == ProcessingJobParameter.processingJobId,
            )
            .filter(ProcessingJobParameter.parameterKey == "program_id")
            .filter(
                or_(
                    ProcessingJob.recipe == "postprocessing-big-ep",
                    ProcessingJob.recipe == "postprocessing-big-ep-setup",
                )
            )
        )
        programids = {data[v]["program_id"]: v for v in data}
        if not programids:
            raise ValueError("program_id values not found")
        if len(programids) == 1:
            query = query.filter(
                ProcessingJobParameter.parameterValue == tuple(programids)[0]
            )
        else:
            query = query.filter(
                ProcessingJobParameter.parameterValue.in_(tuple(programids))
            )
        if last_records:
            query = query.order_by(desc(ProcessingJobParameter.processingJobId)).limit(
                len(programids)
            )
        rows = list(query.distinct().all())

    results = [dict(zip(row.keys(), row)) for row in rows]
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
        if "xia2.multiplex" in str(filename):
            unmerged_filename = Path(v["filepath"]) / "scaled_unmerged.mtz"
        elif "xia2" in str(filename):
            unmerged_filename = Path(v["filepath"]) / v["filename"].replace(
                "_free.mtz", "_scaled_unmerged.mtz"
            )
        elif "autoPROC" in str(filename):
            unmerged_filename = Path(v["filepath"]) / "aimless_unmerged.mtz"
        else:
            print(f"Unrecognised file path {str(filename)}")
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
            "postprocessing-big-ep",
            "--add-param",
            f"program_id:{v['program_id']}",
            "--add-param",
            f"data:{filename}",
            "--add-param",
            f"scaled_unmerged_mtz:{unmerged_filename}",
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
        elif "xia2.multiplex" in filepath:
            path_ext = "xia2.multiplex"
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
            "zocalo.go",
            "-e",
            "test",
            "-s",
            f"path_ext={path_ext}",
            "-s",
            "force=true",
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


def run():
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
