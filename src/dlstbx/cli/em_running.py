import argparse
import time
from datetime import datetime

import ispyb
import ispyb.sqlalchemy
import sqlalchemy
from ispyb.sqlalchemy import (
    CTF,
    AutoProcProgram,
    DataCollection,
    MotionCorrection,
    ParticlePicker,
    ProcessingJob,
)
from sqlalchemy.orm import Load


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", action="store", dest="microscope")
    parser.add_argument("-v", action="store_true", default=False)
    args = parser.parse_args()
    sessions = []
    with ispyb.open("/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg") as i:
        if args.microscope is None:
            beamlines = ["m02", "m03", "m04", "m05", "m06", "m07", "m08", "m11", "m12"]
        else:
            beamlines = [args.microscope]

        try:
            sessions = []
            for beamline in beamlines:
                sessions.extend(
                    i.core.retrieve_sessions_for_beamline_and_run(beamline, None)
                )
        except ispyb.NoResult:
            pass

    url = ispyb.sqlalchemy.url()
    engine = sqlalchemy.create_engine(url, connect_args={"use_pure": True})
    db_session_maker = sqlalchemy.orm.sessionmaker(bind=engine)

    apps = {}
    with db_session_maker() as db_session:
        for sess in sessions:
            query = (
                db_session.query(DataCollection)
                .options(
                    Load(DataCollection).load_only("SESSIONID", "dataCollectionId")
                )
                .filter(DataCollection.SESSIONID == sess["sessionId"])
            )
            datacollections = list(query.all())
            apps[sess["session"]] = []
            for dc in datacollections:
                query = (
                    db_session.query(AutoProcProgram, ProcessingJob)
                    .join(
                        ProcessingJob,
                        ProcessingJob.processingJobId
                        == AutoProcProgram.processingJobId,
                    )
                    .filter(ProcessingJob.dataCollectionId == dc.dataCollectionId)
                )
                apps[sess["session"]].extend(query.all())

    if args.microscope:
        print(f"running jobs for microscope {args.microscope} in the current run: \n")
    else:
        print("running jobs in the current run: \n")
    for sess in sessions:
        msgs = []
        for proc in apps[sess["session"]]:
            if proc[0].processingStatus is None:
                start_time = proc[0].processingStartTime
                age = datetime.fromtimestamp(time.time()) - start_time
                msg = {
                    "progid": proc[0].autoProcProgramId,
                    "pid": proc[0].processingJobId,
                    "days": age.days,
                    "hours": age.seconds // 3600,
                    "mins": (age.seconds // 60) % 60,
                }
                if args.v:
                    with db_session_maker() as db_session:
                        mccount = (
                            db_session.query(MotionCorrection)
                            .options(
                                Load(MotionCorrection).load_only("motionCorrectionId")
                            )
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
                        parpickcount = (
                            db_session.query(ParticlePicker)
                            .options(Load(ParticlePicker).load_only("particlePickerId"))
                            .filter(ParticlePicker.programId == proc[0].programId)
                            .count()
                        )
                    msg.update(
                        {
                            "mcresults": mccount,
                            "ctfresults": ctfcount,
                            "parpickresults": parpickcount,
                        }
                    )
                msgs.append(msg)
        if msgs:
            print(f"session: {sess['session']}:")
            for m in msgs:
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
            print()
