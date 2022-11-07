"""
Identify up to 400 visits that should be marked as archived, and mark them as archived.

The logic is as follows: (originally set out in https://jira.diamond.ac.uk/browse/SCI-9372, slightly modified*)
  If visit hasn't started or is still running: leave alone
  If visit has ended <= 40 days ago: leave alone
  If visit has ended >= 365 days ago: set to archived
  If visit has ended more than 40 then
      If the proposal is closed: set to archived
      If a visit contains no data collections: set to archived
      For all data collections in the visit check if the imageDirectory exists and contains at least one file. If no such data collection is found: set to archived

* This script only checks up to 400 visits in one go. Requests are slowed down to not tax ISPyB too much.
  Checks for closed proposals, absent data collections and absent data can be run on visits older than 365 days
"""


from __future__ import annotations

import datetime
import itertools
from pathlib import Path

import ispyb
import ispyb.sqlalchemy as models
import pydantic
import sqlalchemy.orm
from sqlalchemy import func

Session = sqlalchemy.orm.sessionmaker(
    bind=sqlalchemy.create_engine(models.url(), connect_args={"use_pure": True})
)


class Archivable(pydantic.BaseModel):
    proposal_code: str = pydantic.Field(..., alias="proposalCode")
    proposal_number: pydantic.NonNegativeInt = pydantic.Field(
        ..., alias="proposalNumber"
    )
    visit_number: pydantic.NonNegativeInt


def get_session_archived_counts(
    db_session: sqlalchemy.orm.session.Session,
) -> dict[int, int]:
    query = (
        db_session.query(
            models.BLSession.archived, func.count(models.BLSession.sessionId)
        )
        .join(models.Proposal)
        .group_by(models.BLSession.archived)
    )
    return {row[0]: row[1] for row in query.all()}


def mark_as_archived(archivables: list[Archivable]):
    conn = ispyb.open()
    for visit in archivables:
        print(
            f"Marking visit {visit.proposal_code}{visit.proposal_number}-{visit.visit_number} as archived"
        )
        conn.call_sp_write(
            "update_session_archived",
            (
                visit.proposal_code,
                visit.proposal_number,
                visit.visit_number,
                1,
            ),
        )


def run():

    with Session() as db_session:
        session_archived_counts = get_session_archived_counts(db_session)
        print(
            f"Database contains {session_archived_counts[0]} active and {session_archived_counts[1]} archived visits."
        )

        one_year_ago = datetime.date.today() - datetime.timedelta(days=365)
        forty_days_ago = datetime.date.today() - datetime.timedelta(days=40)

        # Archive visits that ended more than a year ago
        query = (
            db_session.query(
                models.Proposal.proposalCode,
                models.Proposal.proposalNumber,
                models.BLSession.visit_number,
                models.BLSession.endDate,
            )
            .join(models.BLSession)
            .filter(models.BLSession.archived != 1)
            .filter(models.BLSession.endDate < one_year_ago)
            .limit(100)
        )
        archivables = [Archivable(**row) for row in query.all()]
        print(f"Found {len(archivables)} visits that ended more than a year ago")
        mark_as_archived(archivables)

        # Archive visits that ended more than 40 days ago and the proposal is closed
        query = (
            db_session.query(
                models.Proposal.proposalCode,
                models.Proposal.proposalNumber,
                models.BLSession.visit_number,
                models.BLSession.endDate,
            )
            .join(models.BLSession)
            .filter(models.BLSession.archived != 1)
            .filter(models.BLSession.endDate < forty_days_ago)
            .filter(models.Proposal.state == "Closed")
            .limit(100)
        )

        archivables = [Archivable(**row) for row in query.all()]
        print(
            f"Found {len(archivables)} visits that ended more than 40 days ago and have a closed proposal"
        )
        mark_as_archived(archivables)

        # Archive visits that ended more than 40 days ago and contain no data collections
        query = (
            db_session.query(
                models.Proposal.proposalCode,
                models.Proposal.proposalNumber,
                models.BLSession.visit_number,
                models.BLSession.endDate,
            )
            .join(models.BLSession)
            .outerjoin(
                models.DataCollection,
                models.DataCollection.SESSIONID == models.BLSession.sessionId,
            )
            .filter(models.BLSession.archived != 1)
            .filter(models.BLSession.endDate < forty_days_ago)
            .filter(models.DataCollection.dataCollectionId.is_(None))
        )
        archivables = [Archivable(**row) for row in query.all()]
        print(
            f"Found {len(archivables)} visits that ended more than 40 days ago and have no data collections"
        )
        mark_as_archived(archivables)

        # Find all data collections associated with visits that ended more than 40 days ago
        # where the proposal is still open
        query = (
            db_session.query(
                models.BLSession.sessionId,
                models.Proposal.proposalCode,
                models.Proposal.proposalNumber,
                models.BLSession.visit_number,
                models.BLSession.endDate,
                models.DataCollection.imageDirectory,
            )
            .join(models.BLSession)
            .outerjoin(
                models.DataCollection,
                models.DataCollection.SESSIONID == models.BLSession.sessionId,
            )
            .filter(models.BLSession.archived != 1)
            .filter(models.BLSession.endDate < forty_days_ago)
            .filter(models.Proposal.state == "Open")
            .order_by(models.BLSession.sessionId)
            .order_by(models.DataCollection.imageDirectory)
            .distinct()
            # .limit(100)
        )

        # For all data collections in the visit check if the imageDirectory exists
        # and contains at least one file. If no such data collection is found: set
        # to archived
        grouped = itertools.groupby(query.all(), key=lambda x: x[0])
        archivables = []
        for session_id, group in grouped:
            for row in group:
                image_directory = Path(row.imageDirectory)
                files = [
                    f
                    for f in image_directory.iterdir()
                    if f.suffix
                    not in {
                        ".run",
                        ".gridscan",
                    }  # legacy files that aren't removed when a visit is archived
                ]
                if len(files):
                    print(f"Found {files[0]} for {session_id=}")
                    break
            else:
                archivables.append(Archivable(**row))

        print(
            f"Found {len(archivables)} visits that ended more than 40 days ago and have no associated files on disk"
        )
        mark_as_archived(archivables)
