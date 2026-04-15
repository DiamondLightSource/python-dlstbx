from __future__ import annotations

import re
from logging import Logger
from typing import TYPE_CHECKING

import sqlalchemy
from ispyb.sqlalchemy import DataCollection

if TYPE_CHECKING:
    from dlstbx.services.trigger import MetalIdParameters


def dcids_from_related_dcids(
    logger: Logger,
    parameters: "MetalIdParameters",
    session: sqlalchemy.orm.session.Session,
) -> list[int]:
    # I23 specific routine for finding matching data collections
    """
    For metal ID experiments, I23 take a series of data collections where the image prefix
    ends in E#, where # is an integer assigned to each different photon energy used in the
    series of metal ID data collections. This routine looks to find a corresponding data
    collection at E(#-1) for the same sample in the same visit, to match with the current
    data collection and run metal ID. This routine also looks for a matching start image
    number and number of images in the data collections to handle interleaving type
    experiments. If more than one result comes back, the routine checks for a matching data
    collection number to handle cases where multiple sweeps have been taken. If more than
    one match is still found, the most recent one will be used. The expectation is that the
    same number of sweeps and same collection parameters have been used in all data
    collections in the series. For interleaving experiments, a consequence of this routine
    is that metal ID will run for all matching partial data sets but ultimately the metal
    ID pipeline triggered by the final multiplex pipeline call will be the most useful.

    Returns a list of two dcids [previous_dcid, current_dcid] if a match is found,
    otherwise returns an empty list.
    """

    related_dcids = parameters.related_dcids
    dcids = []
    for related_dcid_set in related_dcids:
        if related_dcid_set.sample_id:
            dcids = related_dcid_set.dcids
    if not dcids:
        logger.info(
            f"Skipping metal id trigger: No sample-specific related DCIDs for {parameters.dcid}"
        )
        return []

    dc_info = parameters.dc_info
    if any(
        getattr(dc_info, field) is None
        for field in [
            "imagePrefix",
            "SESSIONID",
            "dataCollectionNumber",
        ]
    ):
        logger.info(
            f"Skipping metal id trigger: dcid info missing for dcid '{parameters.dcid}'"
        )
        return []
    # Regex to check imagePrefix in format <prefix>_E<energy number> and extract information
    pattern = re.compile(r"^(?P<prefix>.*)_E(?P<energy_num>\d+)$")
    assert dc_info.imagePrefix is not None
    if match := pattern.match(dc_info.imagePrefix):
        energy_num = int(match.group("energy_num"))
        prefix_prefix = match.group("prefix")
    else:
        logger.info(
            f"Skipping metal id trigger: Image prefix '{dc_info.imagePrefix}' does not match expected pattern"
        )
        return []

    logger.info(
        f"dcids: '{dcids}', number of images: '{dc_info.numberOfImages}', start image: '{dc_info.startImageNumber}', image prefix: '{dc_info.imagePrefix}', session id: '{dc_info.SESSIONID}', energy num: '{energy_num}', data collection number: '{dc_info.dataCollectionNumber}'"
    )

    query = (
        (session.query(DataCollection))
        .filter(DataCollection.dataCollectionId.in_(dcids))
        .filter(DataCollection.numberOfImages == dc_info.numberOfImages)
        .filter(DataCollection.startImageNumber == dc_info.startImageNumber)
        .filter(DataCollection.imagePrefix == f"{prefix_prefix}_E{energy_num - 1}")
        .filter(DataCollection.SESSIONID == dc_info.SESSIONID)
        .filter(DataCollection.dataCollectionNumber == dc_info.dataCollectionNumber)
    )
    if not len(query.all()):
        logger.info("Skipping metal id trigger: No matching data collections found")
        return []
    elif len(query.all()) == 1:
        return [query[0].dataCollectionId, parameters.dcid]
    else:
        logger.error(
            "Skipping metal ID trigger - found multiple matching data collections. This should not be possible"
        )
        return []
