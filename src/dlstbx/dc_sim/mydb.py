from __future__ import annotations

import configparser
import logging

import ispyb.connector.mysqlsp.main
import sqlalchemy
from ispyb.sqlalchemy import BLSession, DataCollection, Proposal
from sqlalchemy.orm import Load

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


def retrieve_datacollection(db_session, sessionid, path, prefix, run_number):
    records_to_collect = (
        "BLSAMPLEID",
        "FOCALSPOTSIZEATSAMPLEX",
        "FOCALSPOTSIZEATSAMPLEY",
        "axisEnd",
        "axisRange",
        "axisStart",
        "beamSizeAtSampleX",
        "beamSizeAtSampleY",
        "chiStart",
        "comments",
        "dataCollectionGroupId",
        "dataCollectionId",
        "detectorDistance",
        "exposureTime",
        "fileTemplate",
        "flux",
        "imageSuffix",
        "kappaStart",
        "numberOfImages",
        "numberOfPasses",
        "omegaStart",
        "overlap",
        "phiStart",
        "printableForReport",
        "resolution",
        "rotationAxis",
        "runStatus",
        "slitGapHorizontal",
        "slitGapVertical",
        "startImageNumber",
        "synchrotronMode",
        "transmission",
        "undulatorGap1",
        "wavelength",
        "xBeam",
        "xtalSnapshotFullPath1",
        "xtalSnapshotFullPath2",
        "xtalSnapshotFullPath3",
        "xtalSnapshotFullPath4",
        "yBeam",
    )

    query = (
        db_session.query(DataCollection)
        .options(Load(DataCollection).load_only(*records_to_collect))
        .filter(DataCollection.SESSIONID == sessionid)
        .filter(DataCollection.imageDirectory == path + "/")
    )
    if run_number is not None:
        query = query.filter(DataCollection.dataCollectionNumber == run_number)
    if prefix is not None:
        query = query.filter(DataCollection.imagePrefix == prefix)
    result = query.first()
    if not result:
        raise ValueError("No matching data collection found")
    return result


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


def retrieve_dc_from_dcid(db_session, dcid):
    records_to_collect = (
        "BLSAMPLEID",
        "dataCollectionGroupId",
        "dataCollectionId",
        "dataCollectionNumber",
        "imageDirectory",
        "imagePrefix",
        "imageSuffix",
    )

    query = (
        db_session.query(DataCollection)
        .options(Load(DataCollection).load_only(*records_to_collect))
        .filter(DataCollection.dataCollectionId == dcid)
    )
    result = query.first()
    if not result:
        raise ValueError("No matching data collection found")
    return result
