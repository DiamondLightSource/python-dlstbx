import argparse
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
from relion.zocalo.alchemy import ClusterJobInfo, RelionJobInfo, RelionPipelineInfo
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


def _df_to_db(
    df: pd.DataFrame, microscope: str, path: str, appid: Optional[int] = None
):
    image_x = df.iloc[0]["image_size"][0]
    image_y = df.iloc[0]["image_size"][1]
    session_maker = _get_sessionmaker()
    with session_maker() as session:
        insert_cmd = insert(RelionPipelineInfo).values(
            image_x=image_x,
            image_y=image_y,
            microscope=microscope,
            project_path=path,
        )
        pipeline_cursor = session.execute(insert_cmd)
        pid = pipeline_cursor.inserted_primary_key[0]
        session.commit()
        for index, row in df.iterrows():
            if row["cluster_id"] == "N/A":
                cluster_id = None
            else:
                cluster_id = row["cluster_id"]
            if cluster_id:
                insert_cmd = insert(ClusterJobInfo).values(
                    cluster="hamilton",
                    cluster_id=cluster_id,
                    auto_proc_program_id=appid,
                    start_time=row["cluster_start_time"].strftime("%Y-%m-%d %H:%M:%S"),
                    end_time=row["end_time"].strftime("%Y-%m-%d %H:%M:%S"),
                )
                session.execute(insert_cmd)
            insert_cmd = insert(RelionJobInfo).values(
                cluster_id=cluster_id,
                relion_start_time=row["start_time"].strftime("%Y-%m-%d %H:%M:%S"),
                num_micrographs=row["num_mics"],
                job_name=row["job"],
                pipeline_id=pid,
            )
            session.execute(insert_cmd)
            session.commit()


def run() -> None:
    parser = argparse.ArgumentParser()

    all_microscopes = [
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
    parser.add_argument(
        "-m", action="append", dest="microscopes", default=all_microscopes
    )
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

    sessions = {m: [] for m in args.microscopes}
    with ispyb.open("/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg") as i:
        try:
            for mic in args.microscopes:
                sessions[mic].extend(
                    i.core.retrieve_sessions_for_beamline_and_run(mic, last_run)
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
                        print(f"{len(df.index)} jobs found in {autoproc_dir}")
                        _df_to_db(df, m, str(autoproc_dir / "relion"))
                        df_all = pd.concat([df_all, df])
