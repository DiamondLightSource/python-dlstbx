import logging

import ispyb.connector.mysqlsp.main
import configparser
import sqlalchemy
from sqlalchemy.orm import Load
from ispyb.sqlalchemy import BLSession, Proposal

log = logging.getLogger("dlstbx.dc_sim")


class DB:
    def __init__(self):
        config = configparser.RawConfigParser(allow_no_value=True)
        assert config.read("/dls_sw/dasc/mariadb/credentials/ispyb_scripts.cfg")
        credentials = dict(config.items("prod"))
        self.i = ispyb.connector.mysqlsp.main.ISPyBMySQLSPConnector(**credentials)
        self.cursor = self.i.create_cursor()

    def doQuery(self, querystr):
        cursor = self.i.create_cursor()
        log.debug("DB: %s", querystr)
        try:
            cursor.execute(querystr)
            return cursor.fetchall()
        finally:
            cursor.close()


def retrieve_sessionid(db_session, visit):
    query = (
        db_session.query(BLSession, Proposal)
        .options(
            Load(BLSession).load_only("sessionId", "visit_number", "proposalId"),
            Load(Proposal).load_only("proposalId", "proposalCode", "proposalNumber"),
        )
        .join(
            Proposal,
            Proposal.proposalId == BLSession.proposalId,
        )
        .filter(
            sqlalchemy.func.concat(
                Proposal.proposalCode,
                Proposal.proposalNumber,
                "-",
                BLSession.visit_number,
            )
            == visit
        )
    )

    query_results = query.first()
    if query_results is None:
        raise ValueError(f"Query to obtain sessionid failed for {visit}")
    if query_results[0].sessionId is None:
        raise ValueError(f"Could not find sessionid for visit {visit}")
    return query_results[0].sessionId
