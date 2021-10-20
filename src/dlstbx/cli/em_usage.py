import datetime
import json
import pathlib
from typing import Optional

import ispyb.sqlalchemy
import pandas as pd
import sqlalchemy
from ispyb.sqlalchemy import VRun
from relion import Project
from relion.cli import project_timeline
from relion.zocalo.alchemy import ClusterJobInfo, RelionJobInfo
from sqlalchemy.dialects.mysql import insert


def _get_sessionmaker():
    try:
        configuration = pathlib.Path(
            "/dls_sw/apps/zocalo/secrets/sql-zocalo-profiling.json"
        ).read_text()
    except PermissionError:
        configuration = pathlib.Path(
            "/dls_sw/apps/zocalo/secrets/sql-zocalo-readonly.json"
        ).read_text()
    secret_ingredients = json.loads(configuration)
    sqlalchemy_url = "mysql+mysqlconnector://{user}:{passwd}@{host}:{port}/{db}".format(
        **secret_ingredients
    )
    _sessionmaker = sqlalchemy.orm.sessionmaker(
        bind=sqlalchemy.create_engine(sqlalchemy_url, connect_args={"use_pure": True})
    )
    return _sessionmaker


def _df_to_db(df: pd.DataFrame, appid: Optional[int] = None):
    session_maker = _get_sessionmaker()
    with session_maker() as session:
        for index, row in df.iterrows():
            insert_cmd = insert(ClusterJobInfo).values(
                cluster="hamilton",
                cluster_id=row["cluster_id"],
                auto_proc_program_id=appid,
                start_time=row["cluster_start_time"],
                end_time=row["end_time"],
            )
            session.execute(insert_cmd)
            insert_cmd = insert(RelionJobInfo).values(
                cluster_id=row["cluster_id"],
                relion_start_time=row["start_time"],
                num_micrographs=row["num_mics"],
                job_name=row["job"],
            )
            session.execute(insert_cmd)
            session.commit()


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

    job_info = {
        "start_time": [],
        "end_time": [],
        "job": [],
        "schedule": [],
        "cluster_id": [],
        "cluster_type": [],
        "num_mics": [],
        "useful": [],
        "cluster_start_time": [],
        "image_size": [],
    }
    df_all = pd.DataFrame(job_info)

    for m, sess_list in sessions.items():
        for sess in sess_list:
            processed_dir = (
                pathlib.Path("/dls")
                / m
                / "data"
                / str(last_year)
                / sess["session"]
                / "processed"
            )
            for autoproc_dir in processed_dir.glob("*/*"):
                if not autoproc_dir.is_symlink():
                    df = project_timeline._get_dataframe(
                        Project(autoproc_dir / "relion", cluster=True)
                    )
                    if not df.empty:
                        _df_to_db(df)
                        df_all = pd.concat([df_all, df])

    df_all["total_time"] = df_all["total_time"].dt.total_seconds()
    print(
        df_all[["job", "image_size", "total_time"]]
        .groupby(["image_size", "job"])
        .mean()
    )
