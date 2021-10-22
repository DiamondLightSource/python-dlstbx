import itertools

import pandas as pd
import streamlit as st
from relion.zocalo.alchemy import ClusterJobInfo, RelionJobInfo, RelionPipelineInfo
from sqlalchemy.orm.session import sessionmaker

from dlstbx.cli.em_usage_collect import _get_sessionmaker

columns = [
    [
        c
        for c in RelionJobInfo.__table__.columns.keys()
        if c not in ("job_id", "pipeline_id")
    ],
    [
        c
        for c in RelionPipelineInfo.__table__.columns.keys()
        if c not in ("pipeline_id")
    ],
    [
        c
        for c in ClusterJobInfo.__table__.columns.keys()
        if c not in ("cluster", "cluster_id", "auto_proc_program_id")
    ],
]


def _collect_all_data() -> pd.DataFrame:
    _sessionmaker: sessionmaker = _get_sessionmaker()
    df = pd.DataFrame({c: [] for c in list(itertools.chain(*columns))})
    with _sessionmaker() as session:
        query = (
            session.query(RelionJobInfo, RelionPipelineInfo, ClusterJobInfo)
            .join(ClusterJobInfo, ClusterJobInfo.cluster_id == RelionJobInfo.cluster_id)
            .join(
                RelionPipelineInfo,
                RelionPipelineInfo.pipeline_id == RelionJobInfo.pipeline_id,
            )
        )
        query_result = query.all()
    for r in query_result:
        row = {}
        for column_set in columns:
            row.update({c: getattr(r, c) for c in column_set})
        df = df.append(row)
    return df


def run() -> None:
    df = _collect_all_data()
    df = df[df["microscope"] == "m02"]
    st.dataframe(df)


if __name__ == "__main__":
    run()
