from __future__ import annotations

import argparse
import re
from typing import List, Optional

import ispyb.sqlalchemy
import sqlalchemy.orm
from ispyb.sqlalchemy import BLSession, DataCollection, DataCollectionGroup, Proposal

Session = sqlalchemy.orm.sessionmaker(
    bind=sqlalchemy.create_engine(
        ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
    )
)


def get_bl_sessionid_from_visit_name(
    visit_name: str, db_session: sqlalchemy.orm.session.Session
) -> int:
    m = re.match(r"([a-z][a-z])([\d]+)[-]([\d]+)", visit_name)
    assert m is not None
    assert len(m.groups()) == 3
    proposal_code, proposal_number, visit_number = m.groups()
    query = (
        db_session.query(BLSession)
        .join(Proposal)
        .filter(
            Proposal.proposalCode == proposal_code,
            Proposal.proposalNumber == proposal_number,
            BLSession.visit_number == visit_number,
        )
    )
    blsession = query.one()
    return blsession.sessionId


def get_dcids_for_bl_sessionid(
    bl_sessionid: int,
    db_session: sqlalchemy.orm.session.Session,
    experiment_type: Optional[List[str]] = None,
) -> List[int]:
    query = (
        db_session.query(DataCollection)
        .join(DataCollectionGroup)
        .filter(DataCollection.SESSIONID == bl_sessionid)
    )
    if experiment_type:
        query = (
            query.filter(DataCollectionGroup.experimentType.in_(experiment_type))
            .filter(DataCollection.overlap == 0)
            .filter(DataCollection.numberOfImages > 1)
        )
    return [dc.dataCollectionId for dc in query]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reprocess all data collections for a given visit with autoPROC"
    )
    parser.add_argument(
        "visit", type=str, nargs="+", help="The visit for which to re-run autoPROC"
    )
    expt_type_choices = ["SAD", "Mesh", "Osc", "Serial Jet", "Serial Fixed"]
    parser.add_argument(
        "--experiment-type",
        type=lambda s: [
            expt_type_choices[expt_type_choices.index(f)] for f in s.split(",")
        ],
    )
    args = parser.parse_args()
    with Session() as db_session:
        for visit in args.visit:
            bl_sessionid = get_bl_sessionid_from_visit_name(visit, db_session)
            dcids = get_dcids_for_bl_sessionid(
                bl_sessionid,
                db_session,
                experiment_type=args.experiment_type,
            )
            print("\n".join(map(str, dcids)))
