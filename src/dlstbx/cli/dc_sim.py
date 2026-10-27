from __future__ import annotations

import argparse
import getpass
import logging
import os
import re
import shutil
import socket
from datetime import datetime
from pathlib import Path

import ispyb.sqlalchemy
import sqlalchemy
import sqlalchemy.orm
import workflows.transport
import zocalo.configuration
from ispyb.sqlalchemy import (
    BLSession,
    DataCollection,
    DataCollectionGroup,
    GridInfo,
    Proposal,
)
from sqlalchemy.orm import Load

from dlstbx.crud import get_dcids_for_data_collection_group
from dlstbx.util import INDUSTRIAL_CODES, DowngradeErrorsFilter
from dlstbx.util.colorstreamhandler import ColorStreamHandler

logger = logging.getLogger(__name__)


def get_dc_and_grid_info_from_dcid(
    session: sqlalchemy.orm.Session, dcid: int
) -> tuple[DataCollection, GridInfo | None]:
    """Fetch a data collection and its related records in one database query.

    GridInfo is optional and may be associated directly with the data collection
    or, for legacy records, with its data collection group. If both forms exist,
    the direct association is preferred. BLSample is optional; the
    DataCollection value is preferred, falling back to the group for legacy
    records.
    """

    result = (
        session.query(
            DataCollection,
            GridInfo,
        )
        .outerjoin(
            GridInfo, GridInfo.dataCollectionId == DataCollection.dataCollectionId
        )
        .filter(DataCollection.dataCollectionId == dcid)
        .one_or_none()
    )
    if result is None:
        logger.error(f"No data collection found for dcid={dcid}")
        raise ValueError(f"Could not find data collection for dcid={dcid}")

    return result


def get_data_collection_group_from_dcgid(
    db_session: sqlalchemy.orm.Session, dcgid: int
) -> DataCollectionGroup:
    dcg = (
        db_session.query(DataCollectionGroup).filter(
            DataCollectionGroup.dataCollectionGroupId == dcgid
        )
    ).one_or_none()
    if dcg is None:
        logger.error(
            f"No data collection or related ISPyB records found for dcgid={dcgid}"
        )
        raise ValueError(
            f"Could not find a data collection and its related records for dcgid {dcgid}"
        )
    return dcg


def get_data_collection_group_from_dcid(
    db_session: sqlalchemy.orm.Session, dcid: int
) -> DataCollectionGroup:
    dcg = (
        db_session.query(DataCollectionGroup)
        .join(
            DataCollection,
            DataCollection.dataCollectionGroupId
            == DataCollectionGroup.dataCollectionGroupId,
        )
        .filter(DataCollection.dataCollectionId == dcid)
    ).one_or_none()
    if dcg is None:
        logger.error(
            f"No data collection or related ISPyB records found for dcid={dcid}"
        )
        raise ValueError(
            f"Could not find a data collection and its related records for dcgid={dcid}"
        )
    return dcg


def get_bl_session_and_proposal_code_from_dcgid(
    db_session: sqlalchemy.orm.Session, dcgid: int
) -> tuple[str, BLSession]:
    results = (
        db_session.query(Proposal.proposalCode, BLSession)
        .join(BLSession, BLSession.proposalId == Proposal.proposalId)
        .join(DataCollectionGroup, DataCollectionGroup.sessionId == BLSession.sessionId)
        .filter(DataCollectionGroup.dataCollectionGroupId == dcgid)
        .one_or_none()
    )

    if results is None:
        logger.error(f"No proposal code or BLSession found for dcgid={dcgid}")
        raise ValueError(f"Could not find proposal code or BLSession for dcgid={dcgid}")
    return results


def retrieve_sessionid(db_session: sqlalchemy.orm.Session, visit: str) -> int:
    query = (
        db_session.query(BLSession, Proposal)
        .options(
            Load(BLSession).load_only(
                BLSession.sessionId, BLSession.visit_number, BLSession.proposalId
            ),
            Load(Proposal).load_only(
                Proposal.proposalId, Proposal.proposalCode, Proposal.proposalNumber
            ),
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


def copy_data_collection_group(
    source_dcg: DataCollectionGroup,
    new_session_id: int,
    new_start_time: datetime,
    new_end_time: datetime,
) -> DataCollectionGroup:
    """
    Create a copy of a DataCollectionGroup row with different sessionId and times.

    Args:
        source_dcg: The DataCollectionGroup to copy
        new_session_id: The new sessionId for the copy
        new_start_time: The new startTime for the copy
        new_end_time: The new endTime for the copy

    Returns:
        A new DataCollectionGroup object with all attributes copied except
        the ones specified (which are replaced with the new values)
    """
    # Get all columns from the source object
    new_dcg = DataCollectionGroup()

    # Copy all attributes from source to new, except the primary key and fields to update
    for column in DataCollectionGroup.__table__.columns:
        attr_name = column.name

        # Skip the primary key
        if attr_name == "dataCollectionGroupId":
            continue

        # Use new values for these attributes
        if attr_name == "sessionId":
            setattr(new_dcg, attr_name, new_session_id)
        elif attr_name == "startTime":
            setattr(new_dcg, attr_name, new_start_time)
        elif attr_name == "endTime":
            setattr(new_dcg, attr_name, new_end_time)
        else:
            # Copy from source
            setattr(new_dcg, attr_name, getattr(source_dcg, attr_name, None))

    return new_dcg


def copy_data_collection(
    source_dc: DataCollection,
    new_session_id: int,
    new_data_collection_group_id: int,
    **kwargs,
) -> DataCollection:
    """
    Create a copy of a DataCollection row with different sessionId and/or dataCollectionGroupId.

    Args:
        source_dc: The DataCollection to copy
        new_session_id: The new SESSIONID for the copy
        new_data_collection_group_id: The new dataCollectionGroupId for the copy
        **kwargs: Additional attributes to override (e.g., startTime, endTime, comments)

    Returns:
        A new DataCollection object with all attributes copied except
        the ones specified (which are replaced with the new values)
    """
    # Create a new DataCollection object
    new_dc = DataCollection()

    # Copy all attributes from source to new, except the primary key and fields to update
    for column in DataCollection.__table__.columns:
        attr_name = column.name

        # Skip the primary key
        if attr_name == "dataCollectionId":
            continue

        # Use new values for these attributes
        if attr_name == "SESSIONID":
            setattr(new_dc, attr_name, new_session_id)
        elif attr_name == "dataCollectionGroupId":
            setattr(new_dc, attr_name, new_data_collection_group_id)
        elif attr_name in kwargs:
            # Use provided override values
            setattr(new_dc, attr_name, kwargs[attr_name])
        else:
            # Copy from source
            setattr(new_dc, attr_name, getattr(source_dc, attr_name, None))

    return new_dc


def copy_grid_info(
    source_gi: GridInfo,
    new_dcid: int,
    **kwargs,
) -> DataCollection:
    """
    Create a copy of a DataCollection row with different sessionId and/or dataCollectionGroupId.

    Args:
        source_dc: The DataCollection to copy
        new_session_id: The new SESSIONID for the copy
        new_data_collection_group_id: The new dataCollectionGroupId for the copy
        **kwargs: Additional attributes to override (e.g., startTime, endTime, comments)

    Returns:
        A new DataCollection object with all attributes copied except
        the ones specified (which are replaced with the new values)
    """
    # Create a new GridInfo object
    new_gi = GridInfo()

    # Copy all attributes from source to new, except the primary key and fields to update
    for column in GridInfo.__table__.columns:
        attr_name = column.name

        # Skip the primary key
        if attr_name == "gridInfoId":
            continue

        # Use new values for these attributes
        if attr_name == "dataCollectionId":
            setattr(new_gi, attr_name, new_dcid)
        elif attr_name in kwargs:
            # Use provided override values
            setattr(new_gi, attr_name, kwargs[attr_name])
        else:
            # Copy from source
            setattr(new_gi, attr_name, getattr(source_gi, attr_name, None))
    return new_gi


def simulate(dcid_or_dcgid: int, is_dcg: bool = False) -> list[int]:
    """Look up and copy the records needed to simulate data collection."""
    dflt_proposal = "nt44198"

    sessionmaker = sqlalchemy.orm.sessionmaker(
        bind=sqlalchemy.create_engine(
            ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
        )
    )
    with sessionmaker() as session:
        if is_dcg:
            logger.info(
                f"Starting data collection simulation for dcgid={dcid_or_dcgid}"
            )
            dcg = get_data_collection_group_from_dcgid(session, dcid_or_dcgid)
            dcgid = dcid_or_dcgid
            dcids = get_dcids_for_data_collection_group(dcgid, session)
        else:
            logger.info(f"Starting data collection simulation for dcid={dcid_or_dcgid}")
            dcg = get_data_collection_group_from_dcid(session, dcid_or_dcgid)
            dcgid = dcg.dataCollectionGroupId
            dcids = [dcid_or_dcgid]

        proposal_code, bl_session = get_bl_session_and_proposal_code_from_dcgid(
            session, dcgid
        )

        if proposal_code in INDUSTRIAL_CODES:
            logger.error("Source data is industrial; simulation is not allowed")
            raise SystemExit(1)

        beamline = bl_session.beamLineName
        dest_visit_dir, dest_visit = get_beamline_visit_and_dir_for_proposal(
            dflt_proposal, beamline
        )

        now = datetime.now()
        random_uuid = os.urandom(4).hex()  # Generate a random UUID for uniqueness
        dest_dir = (
            dest_visit_dir
            / "tmp"
            / f"{now:%Y-%m-%d}"
            / f"{now:%H}-{now:%M}-{now:%S}-{random_uuid}"
        )
        dest_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created image destination directory {dest_dir}")
        # Create destination directory for JPEG snapshots
        jpgs_dest_dir = (
            dest_visit_dir
            / "jpegs"
            / "tmp"
            / f"{now:%Y-%m-%d}"
            / f"{now:%H}-{now:%M}-{now:%S}-{random_uuid}"
        )
        jpgs_dest_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created snapshot destination directory {jpgs_dest_dir}")
        # Get the session id for the destination visit
        logger.info(f"Finding ISPyB session for destination visit {dest_visit}")
        new_bl_session_id = retrieve_sessionid(session, dest_visit)
        # Create a copy with new sessionId and times
        logger.info("Copying data collection group")
        new_dcg = copy_data_collection_group(
            dcg,
            new_session_id=new_bl_session_id,
            new_start_time=datetime.now(),
            new_end_time=datetime.now(),
        )

        # Add and commit the new DataCollectionGroup
        session.add(new_dcg)
        session.commit()
        new_dcgid = new_dcg.dataCollectionGroupId
        logger.info(f"Created destination DataCollectionGroup dcgid={new_dcgid}")

        logger.info("Loading source data collection and related ISPyB records")
        new_dcids = []
        for dcid in dcids:
            new_dcid = simulate_datacollection(
                session, dcid, dest_dir, jpgs_dest_dir, new_bl_session_id, new_dcgid
            )
            new_dcids.append(new_dcid)
    return new_dcids


def simulate_datacollection(
    db_session: sqlalchemy.orm.Session,
    dcid: int,
    dest_dir: Path,
    jpgs_dest_dir: Path,
    new_bl_session_id: int,
    new_dcgid: int,
) -> int:
    dc, gi = get_dc_and_grid_info_from_dcid(db_session, dcid)
    source_image_dir = Path(dc.imageDirectory)
    start_time = datetime.now()

    # Copy the image files to the new destination directory
    image_suffix = dc.imageSuffix
    if image_suffix == "cbf":
        file_template = dc.fileTemplate
        match = re.search(r"(#+)(?=\.cbf$)", file_template)
        if not match:
            raise ValueError(
                f"File template {file_template} does not contain a valid '#' pattern for image numbering."
            )
        n_digits = len(match.group(1))
        start_image_number = dc.startImageNumber
        number_of_images = dc.numberOfImages
        copied_images = 0
        missing_images = 0
        logger.info(
            f"Copying {number_of_images} CBF images from {source_image_dir} "
            f"to {dest_dir}"
        )
        for image_number in range(
            start_image_number, start_image_number + number_of_images
        ):
            image_filename = (
                file_template[: match.start()]
                + str(image_number).zfill(n_digits)
                + file_template[match.end() :]
            )
            src_image_path = source_image_dir / image_filename
            dest_image_path = dest_dir / image_filename
            if src_image_path.exists():
                shutil.copy2(src_image_path, dest_image_path)
                copied_images += 1
            else:
                logger.warning(
                    f"Source image {src_image_path} does not exist; skipping copy"
                )
                missing_images += 1
        logger.info(
            f"Finished copying CBF images: copied={copied_images}, "
            f"missing={missing_images}"
        )

    elif image_suffix in ["h5", "nxs"]:
        file_prefix = f"{dc.imagePrefix}_{dc.dataCollectionNumber}"
        files = {
            path
            for pattern in (f"{file_prefix}_*.h5", f"{file_prefix}.nxs")
            for path in source_image_dir.glob(pattern)
        }
        logger.info(
            f"Copying {len(files)} HDF5/Nexus file(s) from {source_image_dir} "
            f"to {dest_dir}"
        )
        for file in files:
            shutil.copy2(file, dest_dir)
        logger.info("Finished copying image data files")
    else:
        logger.error(
            f"Unsupported image suffix {image_suffix!r} for source dcid={dcid}"
        )
        raise ValueError(
            f"Invalid image suffix {image_suffix} for source data collection"
        )

        # Create new DataCollection
    new_dc = copy_data_collection(
        dc,
        new_session_id=new_bl_session_id,
        new_data_collection_group_id=new_dcgid,
        startTime=start_time,
        endTime=datetime.now(),
        comments=f"Simulated data collection copied from dcid {dcid}",
        imageDirectory=str(dest_dir) + "/",
    )
    db_session.add(new_dc)
    db_session.commit()
    new_dcid = new_dc.dataCollectionId

    if gi is not None:
        new_gi = copy_grid_info(source_gi=gi, new_dcid=new_dcid)
        db_session.add(new_gi)
        db_session.commit()

        # Mapping of snapshot attributes to their names
    snapshot_attrs = [
        "xtalSnapshotFullPath1",
        "xtalSnapshotFullPath2",
        "xtalSnapshotFullPath3",
        "xtalSnapshotFullPath4",
    ]

    # Copy and update snapshot paths
    copied_snapshots = 0
    missing_snapshots = 0
    for attr_name in snapshot_attrs:
        src_snapshot_path = getattr(dc, attr_name, None)
        if src_snapshot_path is not None:
            src_snapshot = Path(src_snapshot_path)
            if src_snapshot.exists():
                # Create a unique filename for the snapshot
                dest_snapshot_path = jpgs_dest_dir / src_snapshot.name
                shutil.copy2(src_snapshot, dest_snapshot_path)
                # Update the new DataCollection with the new snapshot path
                setattr(new_dc, attr_name, str(dest_snapshot_path))
                copied_snapshots += 1
            else:
                logger.warning(
                    f"Crystal snapshot {src_snapshot} does not exist; skipping copy"
                )
                missing_snapshots += 1

    logger.info(
        f"Created destination DataCollection {new_dc.dataCollectionId}; "
        f"copied snapshots={copied_snapshots}, missing snapshots={missing_snapshots}"
    )
    logger.info(
        f"Data collection simulation for dcid={dcid} completed in "
        f"{(datetime.now() - start_time).total_seconds():.1f} seconds"
    )
    return new_dcid


def get_beamline_visit_and_dir_for_proposal(
    proposal: str, beamline: str
) -> tuple[Path, str]:
    data_root = Path("/dls", beamline, "data", str(datetime.now().year))
    logger.info(f"Searching for proposal {proposal} under {data_root}")
    for cm_dir in data_root.iterdir():
        if cm_dir.name.startswith(proposal):
            dest_visit_dir = cm_dir
            dest_visit = cm_dir.name
            break
    else:
        raise ValueError(
            f"Could not determine destination directory for proposal {proposal}"
        )
    logger.info(f"Using destination visit directory {dest_visit_dir}")
    return dest_visit_dir, dest_visit


def run(args: list[str] | None = None) -> None:
    # Enable logging to console
    console = ColorStreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger("dlstbx").setLevel(logging.INFO)
    for pika_logger in (
        "base_connection",
        "blocking_connection",
        "utils.connection_workflow",
        "utils.io_services_utils",
    ):
        logging.getLogger(f"pika.adapters.{pika_logger}").addFilter(
            DowngradeErrorsFilter()
        )
    logging.getLogger("workflows").setLevel(logging.INFO)
    logging.getLogger("zocalo").setLevel(logging.INFO)
    logging.getLogger().setLevel(logging.WARNING)
    logging.getLogger().addHandler(console)
    zc = zocalo.configuration.from_file()
    zc.activate()
    parser = argparse.ArgumentParser(
        description="Simulate data collection. Takes a dcid as input and copies data and database records to simulate a data collection, then triggers mimas.",
        usage="dlstbx.dc_sim [options] dcid",
    )
    parser.add_argument(
        "dcid_or_dcgid",
        type=int,
        help="Data collection ID or Data collection group ID - To use Data collection group id, set --dcgid option",
    )
    parser.add_argument("--is-dcgid", action="store_true")
    parser.add_argument("--suppress-mimas", action="store_true")
    zc.add_command_line_options(parser)
    workflows.transport.add_command_line_options(parser, transport_argument=True)

    parsed_args = parser.parse_args(args)
    new_dcids = simulate(parsed_args.dcid_or_dcgid, parsed_args.is_dcgid)

    if not parsed_args.suppress_mimas:
        transport = workflows.transport.lookup(parsed_args.transport)()
        transport.connect()
        for new_dcid in new_dcids:
            logger.info(f"Triggering Mimas for dcid={new_dcid}")
            headers = {
                "zocalo.go.user": getpass.getuser(),
                "zocalo.go.host": socket.gethostname(),
            }

            transport.send(
                "processing_recipe",
                {
                    "recipes": ["mimas"],
                    "parameters": {
                        "ispyb_dcid": new_dcid,
                        "event": "start",
                    },
                },
                headers=headers,
            )
            transport.send(
                "processing_recipe",
                {
                    "recipes": ["mimas"],
                    "parameters": {
                        "ispyb_dcid": new_dcid,
                        "event": "end",
                    },
                },
                headers=headers,
            )
        transport.disconnect()


if __name__ == "__main__":
    run()
