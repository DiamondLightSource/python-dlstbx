from __future__ import annotations

import argparse
import datetime
import pathlib
import time

import ispyb
import ispyb.sqlalchemy
import sqlalchemy
from ispyb.sqlalchemy import (
    CTF,
    AutoProcProgram,
    DataCollection,
    MotionCorrection,
    ProcessingJob,
    VRun,
)
from sqlalchemy.orm import Load


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", action="store", dest="microscope")
    parser.add_argument("-v", action="store_true", default=False)
    args = parser.parse_args()
    sessions = []

    url = ispyb.sqlalchemy.url()
    engine = sqlalchemy.create_engine(url, connect_args={"use_pure": True})
    db_session_maker = sqlalchemy.orm.sessionmaker(bind=engine)

    with db_session_maker() as db_session:
        now = datetime.datetime.now()
        query = (
            db_session.query(VRun)
            .filter(VRun.startDate < now)
            .filter(VRun.endDate > now)
        )
        current_run = query.first().run

    run_year = int(current_run.split("-")[0])
    run_number = int(current_run.split("-")[1])
    # there are 5 runs per year
    if run_number == 1:
        last_year = run_year - 1
        last_number = "05"
    else:
        last_year = run_year
        last_number = f"{run_number-1:02d}"
    last_run = f"{last_year}-{last_number}"

    with ispyb.open("/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg") as i:
        if args.microscope is None:
            beamlines = [
                "m02",
                "m03",
                "m04",
                "m05",
                "m06",
                "m07",
                "m08",
                "m10",
                "m11",
                "m12",
            ]
        else:
            beamlines = [args.microscope]

        try:
            sessions = []
            for beamline in beamlines:
                sessions.extend(
                    i.core.retrieve_sessions_for_beamline_and_run(beamline, None)
                )
                sessions.extend(
                    i.core.retrieve_sessions_for_beamline_and_run(beamline, last_run)
                )
        except ispyb.NoResult:
            pass

    apps = {}
    imgdirs = {}
    with db_session_maker() as db_session:
        for sess in sessions:
            query = (
                db_session.query(DataCollection)
                .options(
                    Load(DataCollection).load_only(
                        "SESSIONID", "dataCollectionId", "imageDirectory"
                    )
                )
                .filter(DataCollection.SESSIONID == sess["sessionId"])
            )
            datacollections = list(query.all())
            for dc in datacollections:
                query = (
                    db_session.query(AutoProcProgram, ProcessingJob)
                    .join(
                        ProcessingJob,
                        ProcessingJob.processingJobId
                        == AutoProcProgram.processingJobId,
                    )
                    .filter(ProcessingJob.dataCollectionId == dc.dataCollectionId)
                    .filter(AutoProcProgram.processingStatus == None)  # noqa: E711
                )
                query_result = query.all()
                if query_result:
                    if apps.get(sess["session"]) is None:
                        apps[sess["session"]] = []
                    apps[sess["session"]].extend(query.all())
                    if imgdirs.get(sess["session"]) is None:
                        imgdirs[sess["session"]] = {}
                    imgdirs[sess["session"]][dc.dataCollectionId] = dc.imageDirectory

        if args.microscope:
            print(
                f"running jobs for microscope {args.microscope} in the current run: \n"
            )
        else:
            print("running jobs in the current run: \n")
        msgs = {}
        for sess, procs in apps.items():
            for proc in procs:
                start_time = proc[0].processingStartTime
                if start_time is not None:
                    age = datetime.datetime.fromtimestamp(time.time()) - start_time
                else:
                    age = None
                try:
                    msg = {
                        "progid": proc[0].autoProcProgramId,
                        "pid": proc[0].processingJobId,
                        "days": age.days,
                        "hours": age.seconds // 3600,
                        "mins": (age.seconds // 60) % 60,
                    }
                except AttributeError:
                    msg = {
                        "progid": proc[0].autoProcProgramId,
                        "pid": proc[0].processingJobId,
                        "days": "???",
                        "hours": "???",
                        "mins": "???",
                    }
                if args.v:
                    mccount = (
                        db_session.query(MotionCorrection)
                        .options(Load(MotionCorrection).load_only("motionCorrectionId"))
                        .filter(
                            MotionCorrection.autoProcProgramId
                            == proc[0].autoProcProgramId
                        )
                        .count()
                    )
                    ctfcount = (
                        db_session.query(CTF)
                        .options(Load(CTF).load_only("ctfId"))
                        .filter(CTF.autoProcProgramId == proc[0].autoProcProgramId)
                        .count()
                    )
                    # parpickcount = (
                    #    db_session.query(ParticlePicker)
                    #    .options(Load(ParticlePicker).load_only("particlePickerId"))
                    #    .filter(ParticlePicker.programId == proc[0].autoProcProgramId)
                    #    .count()
                    # )
                    fileglob = pathlib.Path(
                        imgdirs[sess][proc[1].dataCollectionId]
                    ).glob("**/*")
                    most_recent = max(
                        datetime.datetime.fromtimestamp(p.stat().st_mtime)
                        for p in fileglob
                    )
                    tdiff = now - most_recent
                    msg.update(
                        {
                            "mcresults": mccount,
                            "ctfresults": ctfcount,
                            # "parpickresults": parpickcount,
                            "mod_days": tdiff.days,
                            "mod_hours": tdiff.seconds // 3600,
                            "mod_mins": (tdiff.seconds // 60) % 60,
                        }
                    )
                if msgs.get(sess) is None:
                    msgs[sess] = []
                msgs[sess].append(msg)
    if msgs:
        for sess, sess_msgs in msgs.items():
            print(f"session: {sess}:")
            for m in sess_msgs:
                print(f"{'':<10} program ID: {m['progid']}, job ID: {m['pid']}")
                print(
                    f"{'':<15} age: {m['days']} days, {m['hours']} hours, {m['mins']} minutes"
                )
                if m.get("mcresults") is not None:
                    print(f"{'':<15} motion corrected micrographs {m['mcresults']}")
                if m.get("ctfresults") is not None:
                    print(f"{'':<15} CTF estimations {m['ctfresults']}")
                if m.get("parpickresults") is not None:
                    print(
                        f"{'':<15} particles picked from {m['parpickresults']} micrographs"
                    )
                if m.get("mod_days") is not None:
                    print(
                        f"{'':<15} time since last data transfer: {m['mod_days']} days, {m['mod_hours']} hours, {m['mod_mins']} minutes"
                    )
            print()
