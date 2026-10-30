from __future__ import annotations

import os
import re
import shutil
from datetime import datetime
from pathlib import Path

import ispyb.sqlalchemy
import pydantic
import sqlalchemy
import sqlalchemy.orm
from ispyb.sqlalchemy import (
    BLSession,
    DataCollection,
    DataCollectionGroup,
    GridInfo,
    Proposal,
)
from sqlalchemy.orm import Load

from dlstbx.util import INDUSTRIAL_CODES
from dlstbx.wrapper import Wrapper


@pydantic.dataclasses.dataclass(config=pydantic.ConfigDict(extra="allow"))
class DataCollectionInfo:
    dataCollectionGroupId: pydantic.PositiveInt


@pydantic.dataclasses.dataclass
class DCSimParameters:
    beamline: str
    src_visit: str
    src_allowed_visits: list[str]
    dest_allowed_visits: list[str]
    dflt_proposal: str
    dflt_i02_visits: dict[str, int]
    dc_info: DataCollectionInfo
    dcg_dcids: list[pydantic.PositiveInt]
    dcid: pydantic.PositiveInt
    dest_visit: str | None = None
    copy_dcg: bool | None = False


class DCSimWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.dc_sim"

    def retrieve_sessionid(self, db_session: sqlalchemy.orm.Session, visit: str) -> int:
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
        self, db_session: sqlalchemy.orm.Session, dcgid: int, new_session_id: int
    ) -> int:
        """Copy a row from DataCollectionGroup table, replace session id, insert to the database and return new dcgid"""
        now = datetime.now()
        src_dcg = (
            db_session.query(DataCollectionGroup).filter(
                DataCollectionGroup.dataCollectionGroupId == dcgid
            )
        ).one_or_none()
        if src_dcg is None:
            self.log.error(f"No data collection group found for dcgid={dcgid}")
            raise ValueError(f"Could not find data collection group dcgid={dcgid}")

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
                setattr(new_dcg, attr_name, now)
            elif attr_name == "endTime":
                setattr(new_dcg, attr_name, now)
            else:
                # Copy from source
                setattr(new_dcg, attr_name, getattr(src_dcg, attr_name, None))

        # Add and commit new dcg
        db_session.add(new_dcg)
        db_session.commit()
        return new_dcg.dataCollectionGroupId

    def get_dc_and_grid_info_from_dcid(
        self, db_session: sqlalchemy.orm.Session, dcid: int
    ) -> tuple[DataCollection, GridInfo | None]:
        """Fetch a data collection and its related records in one database query.

        GridInfo is optional and may be associated directly with the data collection
        or, for legacy records, with its data collection group. If both forms exist,
        the direct association is preferred. BLSample is optional; the
        DataCollection value is preferred, falling back to the group for legacy
        records.
        """

        result = (
            db_session.query(
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
            self.log.error(f"No data collection found for dcid={dcid}")
            raise ValueError(f"Could not find data collection for dcid={dcid}")

        return result

    def copy_data_collection(
        self,
        db_session: sqlalchemy.orm.Session,
        source_dc: DataCollection,
        new_session_id: int,
        new_data_collection_group_id: int,
        **kwargs,
    ) -> DataCollection:
        """
        Create a copy of a DataCollection row with different sessionId and dataCollectionGroupId.
        and insert into the database

        Args:
            db_session: Database session
            source_dc: The DataCollection to copy
            new_session_id: The new SESSIONID for the copy
            new_data_collection_group_id: The new dataCollectionGroupId for the copy
            **kwargs: Additional attributes to override (e.g., startTime, endTime, comments)

        Returns:
            The new data collection id
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

        # Add and commit new data collection
        db_session.add(new_dc)
        db_session.commit()
        return new_dc.dataCollectionId

    def copy_grid_info(
        self,
        db_session: sqlalchemy.orm.Session,
        source_gi: GridInfo,
        new_dcid: int,
        **kwargs,
    ) -> DataCollection:
        """
        Create a copy of a DataCollection row with different sessionId and/or dataCollectionGroupId.

        Args:
            db_session: Database session
            source_gc: The GridInfo to copy
            new_dcid: The new dcid to link the copy to

        Returns:
            None
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

        # Add and commit new GridInfo
        db_session.add(new_gi)
        db_session.commit()

    def simulate_datacollection(
        self,
        db_session: sqlalchemy.orm.Session,
        dcid: int,
        dest_dir: Path,
        jpgs_dest_dir: Path,
        new_bl_session_id: int,
        new_dcgid: int,
    ) -> tuple[int, bool]:
        dc, gi = self.get_dc_and_grid_info_from_dcid(db_session, dcid)
        is_grid_scan = gi is not None
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
            missing_images = []
            self.log.info(
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
                    missing_images.append(image_number)
            if not copied_images:
                raise FileNotFoundError(
                    f"Image files not found for dcid={dcid}, data may have been archived"
                )
            self.log.info(
                f"Finished copying CBF images: copied={copied_images}, "
                f"missing_images={missing_images}"
            )

        elif image_suffix in ["h5", "nxs"]:
            main_file_path = source_image_dir / dc.fileTemplate
            if not main_file_path.exists():
                raise FileNotFoundError(
                    f"Image files not found for dcid={dcid}, data may have been archived"
                )
            file_prefix = f"{dc.imagePrefix}_{dc.dataCollectionNumber}"
            files = {
                path
                for pattern in (f"{file_prefix}_*.h5", f"{file_prefix}.nxs")
                for path in source_image_dir.glob(pattern)
            }
            self.log.info(
                f"Copying {len(files)} HDF5/Nexus file(s) from {source_image_dir} "
                f"to {dest_dir}"
            )
            for file in files:
                shutil.copy2(file, dest_dir)
            self.log.info("Finished copying image data files")
        else:
            self.log.error(
                f"Unsupported image suffix {image_suffix!r} for source dcid={dcid}"
            )
            raise ValueError(
                f"Invalid image suffix {image_suffix} for source data collection"
            )

        # Extra params to set in new dc
        dc_extra_params = {
            "startTime": start_time,
            "endTime": datetime.now(),
            "comments": f"Simulated data collection copied from dcid {dcid}",
            "imageDirectory": str(dest_dir) + "/",
        }

        # Handle crystal snapshots
        # Mapping of snapshot attributes to their names
        snapshot_attrs = [
            "xtalSnapshotFullPath1",
            "xtalSnapshotFullPath2",
            "xtalSnapshotFullPath3",
            "xtalSnapshotFullPath4",
        ]
        for attr_name in snapshot_attrs:
            src_snapshot_path = getattr(dc, attr_name, None)
            if src_snapshot_path is not None:
                src_snapshot = Path(src_snapshot_path)
                if src_snapshot.exists():
                    # Create a unique filename for the snapshot
                    dest_snapshot_path = jpgs_dest_dir / src_snapshot.name
                    shutil.copy2(src_snapshot, dest_snapshot_path)
                    # Update the new DataCollection with the new snapshot path
                    dc_extra_params[attr_name] = str(dest_snapshot_path)
                else:
                    self.log.warning(
                        f"Crystal snapshot {src_snapshot} does not exist; skipping copy"
                    )

        # Create new DataCollection
        new_dcid = self.copy_data_collection(
            db_session=db_session,
            source_dc=dc,
            new_session_id=new_bl_session_id,
            new_data_collection_group_id=new_dcgid,
            **dc_extra_params,
        )

        if is_grid_scan:
            self.copy_grid_info(db_session=db_session, source_gi=gi, new_dcid=new_dcid)

        self.log.info(f"Created DataCollection {new_dcid}; ")
        self.log.info(
            f"Data collection simulation for dcid={dcid} completed in "
            f"{(datetime.now() - start_time).total_seconds():.1f} seconds"
        )
        return new_dcid, is_grid_scan

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        # Get parameters from the recipe file (as a copy so that changes can be made without affecting anything)
        params = self.recwrap.recipe_step["job_parameters"].copy()

        # A list of placeholder values to check for in params
        placeholders = [
            "{dest_visit}",
            "{copy_dcg}",
        ]

        # Replace any remaining placeholder values in params with None.
        for key, value in params.items():
            if value in placeholders:
                params[key] = None

        params = DCSimParameters(**params)

        beamline = params.beamline
        src_visit = params.src_visit
        dest_visit = params.dest_visit
        dcid = params.dcid
        now = datetime.now()
        random_uuid = os.urandom(4).hex()  # Generate a random UUID for uniqueness

        if src_visit.startswith(tuple(INDUSTRIAL_CODES)) or not src_visit.startswith(
            tuple(params.src_allowed_visits)
        ):
            raise ValueError(f"{src_visit} is not an allowed source visit")

        if dest_visit is not None:
            if not dest_visit.startswith(tuple(params.dest_allowed_visits)):
                raise ValueError(f"{dest_visit} is not an allowed destination visit")
            proposal = dest_visit.split("-")[0]
        else:
            proposal = params.dflt_proposal
        if beamline.startswith("i02"):
            if dest_visit is None:
                try:
                    dest_visit = f"{proposal}-{params.dflt_i02_visits[beamline]}"
                except KeyError:
                    self.log.error("Could not determine which destination visit to use")
                    raise
            dest_visit_dir = Path("/dls/mx/data", proposal, dest_visit)
        else:
            if dest_visit is None:
                for visit_dir in Path(
                    "/dls", beamline, "data", str(now.year)
                ).iterdir():
                    if visit_dir.name.startswith(proposal):
                        dest_visit = visit_dir.name
                        break
            dest_visit_dir = Path("/dls", beamline, "data", str(now.year), dest_visit)
        assert dest_visit_dir.is_dir(), (
            f"Could not find {dest_visit_dir} directory for the specified visit and beamline."
        )

        dcgid = params.dc_info.dataCollectionGroupId
        dcids = [dcid]
        # Set whether to copy entire dcg
        copy_dcg = params.copy_dcg
        if copy_dcg:
            dcids = (
                params.dcg_dcids
                if dcid in params.dcg_dcids
                else [dcid] + params.dcg_dcids
            )
            dcids.sort()

        dest_dir = (
            dest_visit_dir
            / "tmp"
            / f"{now:%Y-%m-%d}"
            / f"{now:%H}-{now:%M}-{now:%S}-{random_uuid}"
        )
        dest_dir.mkdir(parents=True, exist_ok=True)
        self.log.info(f"Created image destination directory {dest_dir}")
        # Create destination directory for JPEG snapshots
        jpgs_dest_dir = (
            dest_visit_dir
            / "jpegs"
            / "tmp"
            / f"{now:%Y-%m-%d}"
            / f"{now:%H}-{now:%M}-{now:%S}-{random_uuid}"
        )
        jpgs_dest_dir.mkdir(parents=True, exist_ok=True)
        self.log.info(f"Created snapshot destination directory {jpgs_dest_dir}")

        sessionmaker = sqlalchemy.orm.sessionmaker(
            bind=sqlalchemy.create_engine(
                ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
            )
        )

        with sessionmaker() as session:
            dest_bl_session_id = self.retrieve_sessionid(session, dest_visit)

            new_dcgid = self.copy_data_collection_group(
                db_session=session,
                dcgid=dcgid,
                new_session_id=dest_bl_session_id,
            )
            grid_scan_dcids: list[int] = []
            for dcid in dcids:
                new_dcid, is_grid_scan = self.simulate_datacollection(
                    db_session=session,
                    dcid=dcid,
                    dest_dir=dest_dir,
                    jpgs_dest_dir=jpgs_dest_dir,
                    new_bl_session_id=dest_bl_session_id,
                    new_dcgid=new_dcgid,
                )
                # 3D grid scans expect all dcids in dcg to be registered before processing
                if is_grid_scan:
                    grid_scan_dcids.append(new_dcid)
                # Trigger processing for non grid scans as soon as they are ready.
                else:
                    self.trigger_mimas_for_dcid(new_dcid)
            # Process grid scans once all new dcids are in the database
            for grid_scan_dcid in grid_scan_dcids:
                self.trigger_mimas_for_dcid(grid_scan_dcid)
        return True

    def trigger_mimas_for_dcid(self, new_dcid):
        self.log.info(f"Triggering mimas start event for dcid={new_dcid}")
        self.recwrap.send_to(
            "mimas",
            {
                "recipes": ["mimas"],
                "parameters": {"ispyb_dcid": new_dcid, "event": "start"},
            },
        )
        self.log.info(f"Triggering mimas end event for dcid={new_dcid}")
        self.recwrap.send_to(
            "mimas",
            {
                "recipes": ["mimas"],
                "parameters": {"ispyb_dcid": new_dcid, "event": "end"},
            },
        )
