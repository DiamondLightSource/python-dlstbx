import argparse
from typing import List, Optional

import pandas as pd
from relion.zocalo.alchemy import ClusterJobInfo, RelionJobInfo, RelionPipelineInfo
from sqlalchemy.orm import Load
from sqlalchemy.orm.session import sessionmaker

from dlstbx.cli.em_usage_collect import _get_sessionmaker


def _db_to_df(columns: List[str], values: Optional[dict] = None) -> pd.DataFrame:
    _sessionmaker: sessionmaker = _get_sessionmaker()
    pipeline_columns = [
        c for c in columns if c in RelionPipelineInfo.__table__.columns.keys()
    ]
    pipeline_values = {
        k: v
        for k, v in values.items()
        if k in RelionPipelineInfo.__table__.columns.keys()
    }
    job_columns = [c for c in columns if c in RelionJobInfo.__table__.columns.keys()]
    job_values = {
        k: v for k, v in values.items() if k in RelionJobInfo.__table__.columns.keys()
    }
    cluster_columns = [
        c for c in columns if c in ClusterJobInfo.__table__.columns.keys()
    ]
    cluster_values = {
        k: v for k, v in values.items() if k in ClusterJobInfo.__table__.columns.keys()
    }
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
    filters = (
        [getattr(RelionJobInfo, k) == v for k, v in job_values.items()]
        + [getattr(ClusterJobInfo, k) == v for k, v in cluster_values.items()]
        + [getattr(RelionPipelineInfo, k) == v for k, v in pipeline_values.items()]
    )
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
            query = query.filter(*filters)

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
    if all(x in columns for x in ("relion_start_time", "end_time")):
        df["total_time"] = df["end_time"] - df["relion_start_time"]
    if all(x in columns for x in ("start_time", "end_time")):
        df["run_time"] = df["end_time"] - df["start_time"]
    if all(x in columns for x in ("start_time", "end_time", "relion_start_time")):
        df["queue_time"] = df["total_time"] - df["run_time"]
    return df


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--average", action="append", dest="average_these")
    parser.add_argument("-g", "--group", action="append", dest="group_by")
    parser.add_argument(
        "-s",
        help="Set a key, value pair of options as key=value",
        action="append",
        dest="values",
    )
    args = parser.parse_args()

    extras = []
    if "total_time" in args.average:
        extras.extend(["relion_start_time", "end_time"])
    if "run_time" in args.average:
        extras.extend(["start_time", "end_time"])
    if "queue_time" in args.average:
        extras.extend(["start_time", "end_time", "relion_start_time"])

    extras = list(set(extras))

    values_dict = {}
    if args.values:
        values_list = [p.split("=") for p in args.values]
        values_dict = {p[0]: p[1] for p in values_list}
        for k, v in values_dict.items():
            if v.isnumeric():
                values_dict[k] = int(v)
            elif v.lower() == "true":
                values_dict[k] = True
            elif v.lower() == "false":
                values_dict[k] = False
            else:
                try:
                    values_dict[k] = float(v)
                except ValueError:
                    pass

    df = _db_to_df(args.group_by + args.average_these + extras, values=values_dict)

    for time_metric in ("total_time", "run_time", "queue_time"):
        if time_metric in df.columns:
            df[time_metric] = df[time_metric].dt.total_seconds()

    print(df.groupby(args.group_by).mean())
    print(df.groupby(args.group_by).std())
