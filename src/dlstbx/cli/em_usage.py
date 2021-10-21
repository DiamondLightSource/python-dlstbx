import argparse
from typing import List, Optional

import pandas as pd
from relion.zocalo.alchemy import RelionJobInfo
from sqlalchemy.orm import Load
from sqlalchemy.orm.session import sessionmaker

from dlstbx.cli.em_usage_collect import _get_sessionmaker


def _db_to_df(columns: List[str], values: Optional[list] = None) -> pd.DataFrame:
    _sessionmaker: sessionmaker = _get_sessionmaker()
    with _sessionmaker() as session:
        if values:
            query = (
                session.query(RelionJobInfo)
                .options(Load(RelionJobInfo).load_only(*columns))
                .filter(
                    *[getattr(RelionJobInfo, c) == v for c, v in zip(columns, values)]
                )
            )
        else:
            query = session.query(RelionJobInfo).options(
                Load(RelionJobInfo).load_only(*columns)
            )

        query_result = query.all()
    df = pd.DataFrame({c: [] for c in columns})
    for r in query_result:
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
