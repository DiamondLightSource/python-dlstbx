import argparse
from typing import List, Optional

import pandas as pd
from relion.zocalo.alchemy import ClusterJobInfo, RelionJobInfo, RelionPipelineInfo
from sqlalchemy.orm import Load
from sqlalchemy.orm.session import sessionmaker

from dlstbx.cli.em_usage_collect import _get_sessionmaker


def _db_to_df(columns: List[str], values: Optional[list] = None) -> pd.DataFrame:
    _sessionmaker: sessionmaker = _get_sessionmaker()
    pipeline_columns = [
        c for c in columns if c in RelionPipelineInfo.__table__.columns.keys()
    ]
    job_columns = [c for c in columns if c in RelionJobInfo.__table__.columns.keys()]
    cluster_columns = [
        c for c in columns if c in ClusterJobInfo.__table__.columns.keys()
    ]
    extras = [
        c
        for c, p in zip(
            ["cluster_id", "pipeline_id"], [cluster_columns, pipeline_columns]
        )
        if p
    ]
    tables = [
        c
        for c, p in zip(
            [RelionJobInfo, RelionPipelineInfo, ClusterJobInfo],
            [True, pipeline_columns, cluster_columns],
        )
        if p
    ]
    with _sessionmaker() as session:
        query = session.query(*tables).options(
            Load(RelionJobInfo).load_only(*(job_columns + extras))
        )
        if pipeline_columns:
            query = query.options(
                Load(RelionPipelineInfo).load_only(
                    *(pipeline_columns + ["pipeline_id"])
                ),
            ).join(
                RelionPipelineInfo,
                RelionPipelineInfo.pipeline_id == RelionJobInfo.pipeline_id,
            )
        if cluster_columns:
            query = query.options(
                Load(ClusterJobInfo).load_only(*(cluster_columns + ["cluster_id"])),
            ).join(
                ClusterJobInfo,
                ClusterJobInfo.cluster_id == RelionJobInfo.cluster_id,
            )

        if values:
            query = query.filter(
                *[getattr(RelionJobInfo, c) == v for c, v in zip(job_columns, values)]
            )

        query_result = query.all()
    df = pd.DataFrame({c: [] for c in columns})
    for r in query_result:
        if pipeline_columns and cluster_columns:
            df = df.append(
                {
                    c: (getattr(r[1], c) or 0)
                    if c in pipeline_columns
                    else (getattr(r[2], c) or 0)
                    if c in cluster_columns
                    else (getattr(r[0], c) or 0)
                    for c in columns
                },
                ignore_index=True,
            )
        elif pipeline_columns:
            df = df.append(
                {
                    c: (getattr(r[1], c) or 0)
                    if c in pipeline_columns
                    else (getattr(r[0], c) or 0)
                    for c in columns
                },
                ignore_index=True,
            )
        elif cluster_columns:
            df = df.append(
                {
                    c: (getattr(r[1], c) or 0)
                    if c in cluster_columns
                    else (getattr(r[0], c) or 0)
                    for c in columns
                },
                ignore_index=True,
            )
        else:
            df = df.append({c: getattr(r, c) or 0 for c in columns}, ignore_index=True)
    return df


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--average", action="append", dest="average_these")
    parser.add_argument("-g", "--group", action="append", dest="group_by")
    args = parser.parse_args()

    df = _db_to_df(args.group_by + args.average_these)

    print(df.groupby(args.group_by).mean())
    print(df.groupby(args.group_by).std())
