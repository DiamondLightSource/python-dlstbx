from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import ispyb.sqlalchemy
import sqlalchemy
import sqlalchemy.orm
from ispyb.sqlalchemy import (
    BLSample,
    BLSession,
    DataCollection,
    DataCollectionGroup,
    GridInfo,
    Proposal,
)
from sqlalchemy.orm import Load

from dlstbx.util import INDUSTRIAL_CODES

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DataCollectionRecords:
    """Related ISPyB records needed to simulate one data collection."""

    data_collection: DataCollection
    data_collection_group: DataCollectionGroup
    bl_session: BLSession
    proposal: Proposal
    grid_info: GridInfo | None
    bl_sample: BLSample | None


def big_query(session: sqlalchemy.orm.Session, dcid: int) -> DataCollectionRecords:
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
            DataCollectionGroup,
            BLSession,
            Proposal,
            GridInfo,
            BLSample,
        )
        .join(
            DataCollectionGroup,
            DataCollection.dataCollectionGroupId
            == DataCollectionGroup.dataCollectionGroupId,
        )
        .join(BLSession, DataCollectionGroup.sessionId == BLSession.sessionId)
        .join(Proposal, BLSession.proposalId == Proposal.proposalId)
        .outerjoin(
            GridInfo, GridInfo.dataCollectionId == DataCollection.dataCollectionId
        )
        .outerjoin(
            BLSample,
            BLSample.blSampleId
            == sqlalchemy.func.coalesce(
                DataCollection.BLSAMPLEID, DataCollectionGroup.blSampleId
            ),
        )
        .filter(DataCollection.dataCollectionId == dcid)
        .one_or_none()
    )
    if result is None:
        logger.error(
            f"No data collection or related ISPyB records found for dcid={dcid}"
        )
        raise ValueError(
            f"Could not find a data collection and its related records for dcid {dcid}"
        )

    return DataCollectionRecords(*result)


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


def get_data_collection_group_id(
    db_session: sqlalchemy.orm.Session, dcgid: int
) -> sqlalchemy.orm.Query:
    return (
        db_session.query(DataCollectionGroup)
        .filter(DataCollectionGroup.dataCollectionGroupId == dcgid)
        .first()
    )


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


def simulate_data_collection(dcid: int) -> None:
    """Look up and copy the records needed to simulate data collection."""
    start_time = datetime.now()
    dflt_proposal = "nt44198"
    logger.info(f"Starting data collection simulation for dcid={dcid}")

    sessionmaker = sqlalchemy.orm.sessionmaker(
        bind=sqlalchemy.create_engine(
            ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
        )
    )
    with sessionmaker() as session:
        logger.info("Loading source data collection and related ISPyB records")
        dc_records = big_query(session, dcid)

        if dc_records.proposal.proposalCode in INDUSTRIAL_CODES:
            logger.error(
                f"Source data collection {dcid} is industrial; simulation is not allowed"
            )
            raise SystemExit(1)

        beamline = dc_records.bl_session.beamLineName
        dest_visit_dir, dest_visit = get_beamline_visit_and_dir_for_proposal(
            dflt_proposal, beamline
        )

        dc = dc_records.data_collection
        dcg = dc_records.data_collection_group
        logger.info(
            f"Source records loaded: dcid={dcid}, beamline={beamline}, "
            f"destination visit={dest_visit}"
        )

        now = datetime.now()
        source_image_dir = Path(dc.imageDirectory)
        random_uuid = os.urandom(4).hex()  # Generate a random UUID for uniqueness
        dest_dir = (
            dest_visit_dir
            / "tmp"
            / f"{now:%Y-%m-%d}"
            / f"{now:%H}-{now:%M}-{now:%S}-{random_uuid}"
        )
        dest_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created image destination directory {dest_dir}")
        # Get the session id for the destination visit
        logger.info(f"Finding ISPyB session for destination visit {dest_visit}")
        new_bl_session_id = retrieve_sessionid(session, dest_visit)
        # Create a copy with new sessionId and times
        new_dcg = copy_data_collection_group(
            dcg,
            new_session_id=new_bl_session_id,
            new_start_time=start_time,
            new_end_time=datetime.now(),
        )

        # Add and commit the new DataCollectionGroup
        session.add(new_dcg)
        session.commit()
        logger.info(
            f"Created destination DataCollectionGroup {new_dcg.dataCollectionGroupId}"
        )

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
            new_data_collection_group_id=new_dcg.dataCollectionGroupId,
            startTime=start_time,
            endTime=datetime.now(),
            comments=f"Simulated data collection copied from dcid {dcid}",
            imageDirectory=str(dest_dir) + "/",
        )

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

        session.add(new_dc)
        session.commit()
        logger.info(
            f"Created destination DataCollection {new_dc.dataCollectionId}; "
            f"copied snapshots={copied_snapshots}, missing snapshots={missing_snapshots}"
        )
        logger.info(
            f"Data collection simulation for dcid={dcid} completed in "
            f"{(datetime.now() - start_time).total_seconds():.1f} seconds"
        )


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
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(
        description="Simulate data collection. Takes a dcid as input and copies data and database records to simulate a data collection."
    )
    parser.add_argument("dcid", type=int, help="Data collection ID")
    parsed_args = parser.parse_args(args)

    simulate_data_collection(parsed_args.dcid)


if __name__ == "__main__":
    run()
