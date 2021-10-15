import datetime
import pathlib

import ispyb.sqlalchemy
import sqlalchemy
from ispyb.sqlalchemy import VRun
from relion.cli import project_timeline


def run() -> None:
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
    sessions = {b: [] for b in beamlines}
    with ispyb.open("/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg") as i:
        try:
            for beamline in beamlines:
                sessions[beamline].extend(
                    i.core.retrieve_sessions_for_beamline_and_run(beamline, last_run)
                )
        except ispyb.NoResult:
            pass

    with db_session_maker() as db_session:
        for m, sess_list in sessions.items():
            for sess in sess_list:
                processed_dir = (
                    pathlib.Path("/dls")
                    / m
                    / "data"
                    / str(last_year)
                    / f"{sess['projectCode']}-{sess['visit_number']}"
                    / "processed"
                )
                for autoproc_dir in processed_dir.glob("*/*"):
                    if not autoproc_dir.is_symlink():
                        project_timeline._get_dataframe(autoproc_dir)
