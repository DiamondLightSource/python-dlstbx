from __future__ import annotations

import concurrent.futures
import dataclasses
import decimal
import hashlib
import itertools
import logging
import os
import pathlib
import re
import uuid
from typing import Any, Optional, Tuple, Union

import gemmi
import ispyb.sqlalchemy as isa
import ispyb.sqlalchemy as models
import marshmallow.fields
import sqlalchemy
import yaml
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from sqlalchemy import select
from sqlalchemy.orm import aliased, selectinload, sessionmaker

from dlstbx import crud
from dlstbx.util.pdb import PDBFileOrCode

logger = logging.getLogger("dlstbx.ispybtbx")


def _get(obj: Any, name: str):
    """
    Fetch a named attribute via property or dict lookup.

    Intended to be used so we can mix old/new model schema usage.
    """
    try:
        return getattr(obj, name)
    except AttributeError:
        return obj.get(name)


Session = sessionmaker(
    bind=sqlalchemy.create_engine(isa.url(), connect_args={"use_pure": True})
)


def setup_marshmallow_schema(session):
    # https://marshmallow-sqlalchemy.readthedocs.io/en/latest/recipes.html#automatically-generating-schemas-for-sqlalchemy-models
    for class_ in isa.Base.registry._class_registry.values():
        if hasattr(class_, "__tablename__"):

            class Meta:
                model = class_
                sqla_session = session
                load_instance = True
                include_fk = True

            TYPE_MAPPING = SQLAlchemyAutoSchema.TYPE_MAPPING.copy()
            TYPE_MAPPING.update({decimal.Decimal: marshmallow.fields.Float})
            schema_class_name = "%sSchema" % class_.__name__
            schema_class = type(
                schema_class_name,
                (SQLAlchemyAutoSchema,),
                {
                    "Meta": Meta,
                    "TYPE_MAPPING": TYPE_MAPPING,
                },
            )
            setattr(class_, "__marshmallow__", schema_class)


re_visit_base = re.compile(r"^(.*\/([a-z][a-z][0-9]+-[0-9]+))\/")


class ispybtbx:
    def __init__(self):
        with Session() as session:
            setup_marshmallow_schema(session)
        self.log = logging.getLogger("dlstbx.ispybtbx")
        self.log.debug("ISPyB objects set up")

    def __call__(self, message, parameters, session: sqlalchemy.orm.session.Session):
        reprocessing_id = parameters.get(
            "ispyb_reprocessing_id", parameters.get("ispyb_process")
        )
        if reprocessing_id:

            def ispyb_image_path(data_collection, start, end):
                file_template_full = os.path.join(
                    data_collection.imageDirectory, data_collection.fileTemplate
                )
                if not file_template_full:
                    return None
                if "#" in file_template_full:
                    file_template_full = (
                        re.sub(
                            r"#+",
                            lambda x: "%%0%dd" % len(x.group(0)),
                            file_template_full.replace("%", "%%"),
                            count=1,
                        )
                        % start
                    )
                return f"{file_template_full}:{start:d}:{end:d}"

            parameters["ispyb_process"] = reprocessing_id
            query = (
                session.query(isa.ProcessingJob)
                .options(
                    selectinload(isa.ProcessingJob.ProcessingJobParameters),
                    selectinload(isa.ProcessingJob.ProcessingJobImageSweeps)
                    .selectinload(isa.ProcessingJobImageSweep.DataCollection)
                    .load_only(
                        isa.DataCollection.imageDirectory,
                        isa.DataCollection.fileTemplate,
                    ),
                )
                .filter(isa.ProcessingJob.processingJobId == reprocessing_id)
            )
            rp = query.first()
            if not rp:
                self.log.warning(f"Reprocessing ID {reprocessing_id} not found")
            parameters["ispyb_images"] = ",".join(
                ispyb_image_path(sweep.DataCollection, sweep.startImage, sweep.endImage)
                for sweep in rp.ProcessingJobImageSweeps
            )
            # ispyb_reprocessing_parameters is the deprecated method of
            # accessing the processing parameters
            parameters["ispyb_reprocessing_parameters"] = {
                p.parameterKey: p.parameterValue for p in rp.ProcessingJobParameters
            }
            # ispyb_processing_parameters is the preferred method of
            # accessing the processing parameters
            processing_parameters: dict[str, list[str]] = {}
            for p in rp.ProcessingJobParameters:
                processing_parameters.setdefault(p.parameterKey, [])
                processing_parameters[p.parameterKey].append(p.parameterValue)
            parameters["ispyb_processing_parameters"] = processing_parameters
            schema = isa.ProcessingJob.__marshmallow__()
            parameters["ispyb_processing_job"] = schema.dump(rp)
            if "ispyb_dcid" not in parameters:
                parameters["ispyb_dcid"] = rp.dataCollectionId

        return message, parameters

    def get_gridscan_info(
        self, dcid: int, dcgid: int, session: sqlalchemy.orm.session.Session
    ):
        """Extract GridInfo table contents for a DC group ID."""
        gridinfo = crud.get_gridinfo_for_dcid(dcid, dcgid, session)
        if not gridinfo:
            return {}
        schema = isa.GridInfo.__marshmallow__()
        return schema.dump(gridinfo)

    def get_dc_info(self, dcid: int, session: sqlalchemy.orm.session.Session):
        dc = crud.get_data_collection(dcid, session)
        if dc is None:
            return {}
        schema = isa.DataCollection.__marshmallow__()
        return schema.dump(dc)

    def get_beamline_from_dcid(
        self, dcid: int, session: sqlalchemy.orm.session.Session
    ):
        if bs := crud.get_blsession_for_dcid(dcid, session):
            return bs.beamLineName

    def dc_info_to_detectorclass(
        self, dc_info, session: sqlalchemy.orm.session.Session
    ):
        det_id = dc_info.get("detectorId")
        if det_id is not None and (det := crud.get_detector(det_id, session)):
            if det.detectorModel.lower().startswith("eiger"):
                return "eiger"
            elif det.detectorModel.lower().startswith("pilatus"):
                return "pilatus"

        # Fallback on examining the file extension if nothing recorded in ISPyB
        template = dc_info.get("fileTemplate")
        if not template:
            return None
        if template.endswith("master.h5"):
            return "eiger"
        elif template.endswith(".cbf"):
            return "pilatus"

    def get_sample_group_dcids(
        self,
        ispyb_info,
        session: sqlalchemy.orm.session.Session,
        io_timeout: float = 10,
    ):
        # Test dcid: 5469646
        #      blsampleid: 3065377
        #      blsamplegroupids: 307, 310, 313
        dcid = ispyb_info.get("ispyb_dcid")
        sessionid = ispyb_info.get("ispyb_dc_info", {}).get("SESSIONID")
        if not dcid or not sessionid:
            return []

        this_dc = aliased(isa.DataCollection)
        other_dc = aliased(isa.DataCollection)
        blsg_has_bls1 = aliased(isa.BLSampleGroupHasBLSample)
        blsg_has_bls2 = aliased(isa.BLSampleGroupHasBLSample)
        related_dcids = []
        query = (
            session.query(isa.BLSampleGroup, other_dc.dataCollectionId)
            .join(blsg_has_bls1)
            .join(this_dc, this_dc.BLSAMPLEID == blsg_has_bls1.blSampleId)
            .join(
                blsg_has_bls2,
                blsg_has_bls2.blSampleGroupId == blsg_has_bls1.blSampleGroupId,
            )
            .join(other_dc, other_dc.BLSAMPLEID == blsg_has_bls2.blSampleId)
            .filter(this_dc.dataCollectionId == dcid)
        )
        # Group results by BLSampleGroup
        for sample_group, group in itertools.groupby(
            query.all(), lambda r: r.BLSampleGroup
        ):
            related_dcids.append(
                {
                    "dcids": [item.dataCollectionId for item in group],
                    "sample_group_id": sample_group.blSampleGroupId,
                    "name": sample_group.name,
                }
            )

        logger.debug(
            f"dcids defined via BLSampleGroup for dcid={dcid}: {related_dcids}"
        )

        # Else look for sample groups defined in
        # ${visit}/processing/sample_groups.yml, e.g.
        #   $ cat ${visit}/processing/sample_groups.yml
        #     - [well_10, well_11, well_12]
        #     - [well_121, well_122, well_124, well_126, well_146, well_150]
        if not related_dcids:
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(load_sample_group_config_file, ispyb_info)
                    sample_groups = future.result(timeout=io_timeout)
            except concurrent.futures.TimeoutError:
                raise
            except Exception as e:
                logger.warning(
                    f"Error loading sample group config file for {ispyb_info['ispyb_visit']}: {e}",
                    exc_info=True,
                )
            else:
                logger.debug(sample_groups)
                if sample_groups:
                    query = session.query(
                        isa.DataCollection.dataCollectionId,
                        isa.DataCollection.imageDirectory,
                        isa.DataCollection.fileTemplate,
                    ).filter(isa.DataCollection.SESSIONID == sessionid)
                    matches = query.all()
                    for sample_group in sample_groups:
                        sample_group_dcids = []
                        visit_dir = ispyb_info["ispyb_visit_directory"]
                        for dcid, image_directory, template in matches:
                            parts = os.path.relpath(image_directory, visit_dir).split(
                                os.sep
                            )
                            logger.debug(f"parts: {parts}, template: {template}")
                            for prefix in sample_group:
                                if prefix in parts:
                                    sample_group_dcids.append(dcid)
                        related_dcids.append({"dcids": sample_group_dcids})
                logger.debug(
                    f"dcids defined via sample_group.yml for dcid={dcid}: {related_dcids}"
                )
        return related_dcids

    def get_sample_dcids(self, sample_id, session: sqlalchemy.orm.session.Session):
        if not sample_id:
            return None
        dcids = crud.get_dcids_for_sample_id(sample_id, session)
        if dcids:
            sample = crud.get_blsample(sample_id, session)
            related_dcids = {
                "dcids": dcids,
                "sample_id": sample_id,
                "name": sample.name if sample else None,
            }
            logger.debug(
                f"dcids defined via BLSample for {sample_id=}: {related_dcids}"
            )
            return related_dcids

    def get_related_dcids_same_directory(
        self, dcid: int, session: sqlalchemy.orm.session.Session
    ):
        if dcid:
            return {"dcids": crud.get_dcids_for_same_directory(dcid, session)}

    def get_dcg_dcids(
        self, dcid: int, dcgid: int, session: sqlalchemy.orm.session.Session
    ):
        return [
            dcid_
            for dcid_ in crud.get_dcids_for_data_collection_group(dcgid, session)
            if dcid_ != dcid
        ]

    def get_dcg_experiment_type(
        self, dcgid: int, session: sqlalchemy.orm.session.Session
    ) -> Optional[str]:
        if not dcgid:
            return None
        query = session.query(isa.DataCollectionGroup.experimentType).filter(
            isa.DataCollectionGroup.dataCollectionGroupId == dcgid
        )
        return query.one()[0]

    def get_space_group_and_unit_cell(
        self, dcid: int, session: sqlalchemy.orm.session.Session
    ):
        c = crud.get_crystal_for_dcid(dcid, session)
        if not c or not c.spaceGroup:
            return None, None
        proto_cell = (
            c.cell_a,
            c.cell_b,
            c.cell_c,
            c.cell_alpha,
            c.cell_beta,
            c.cell_gamma,
        )
        cell: Union[bool, Tuple[float, ...]]
        if not all(proto_cell):
            cell = False
        else:
            cell = tuple(float(p) for p in proto_cell)
        return c.spaceGroup, cell

    def get_space_group_and_unit_cell_from_yaml(
        self,
        ispyb_info: dict,
        io_timeout: float = 10,
    ):
        dcid = ispyb_info["ispyb_dcid"]
        space_group = None
        unit_cell = None
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(load_configuration_file, ispyb_info)
                params = future.result(timeout=io_timeout)
        except concurrent.futures.TimeoutError:
            raise
        except Exception as exc:
            logger.warning(
                f"Error loading configuration file for dcid={dcid}:\n{exc}",
                exc_info=True,
            )
        else:
            if params:
                space_group = params.get("ispyb_space_group")
                if space_group is not None:
                    try:
                        # Check we have a valid space group, else ignore
                        gemmi.SpaceGroup(space_group)
                    except Exception:
                        logger.warning(
                            f"Can't interpret space group: {space_group} (dcid: {dcid})",
                            exc_info=True,
                        )
                        space_group = None
                unit_cell = params.get("ispyb_unit_cell")
                if isinstance(unit_cell, str):
                    try:
                        unit_cell = [
                            float(p) for p in unit_cell.replace(",", " ").split()
                        ]
                    except ValueError:
                        logger.warning(
                            f"Can't interpret unit cell: {unit_cell} (dcid: {dcid})"
                        )
                        unit_cell = None
                if unit_cell is not None:
                    try:
                        # Check we have a valid unit cell, else ignore
                        gemmi.UnitCell(*unit_cell)
                    except Exception:
                        logger.warning(
                            f"Can't interpret unit cell: {unit_cell} (dcid: {dcid})",
                            exc_info=True,
                        )
                        unit_cell = None

        return space_group, unit_cell

    def get_energy_scan_from_dcid(self, dcid, session: sqlalchemy.orm.session.Session):
        def __energy_offset(row):
            energy = 12398.42 / row.wavelength
            pk_energy = row.EnergyScan.peakEnergy
            if_energy = row.EnergyScan.inflectionEnergy

            return min(abs(pk_energy - energy), abs(if_energy - energy))

        def __select_edge_position(wl, peak, infl):
            e0 = 12398.42 / wl
            if e0 > peak + 30.0:
                return "hrem"
            if e0 < infl - 30.0:
                return "lrem"
            if abs(e0 - infl) < abs(e0 - peak):
                return "infl"
            return "peak"

        this_sample = aliased(isa.BLSample, name="this_sample")
        other_sample = aliased(isa.BLSample, name="other_sample")
        this_crystal = aliased(isa.Crystal)
        other_crystal = aliased(isa.Crystal)
        query = (
            session.query(
                isa.EnergyScan,
                isa.DataCollection.wavelength,
                this_sample,
                other_sample,
            )
            .join(this_sample, this_sample.blSampleId == isa.DataCollection.BLSAMPLEID)
            .join(this_crystal)
            .join(isa.Protein)
            .join(other_crystal, other_crystal.proteinId == isa.Protein.proteinId)
            .join(other_sample, other_sample.crystalId == other_crystal.crystalId)
            .join(
                isa.EnergyScan,
                (isa.EnergyScan.sessionId == isa.DataCollection.SESSIONID)
                & (isa.EnergyScan.blSampleId == other_sample.blSampleId),
            )
            .filter(
                (isa.DataCollection.dataCollectionId == dcid)
                & (isa.EnergyScan.element.isnot(None))
            )
        )
        all_rows = query.all()

        try:
            rows = [
                r
                for r in all_rows
                if r.this_sample.blSampleId == r.other_sample.blSampleId
            ]
            if not rows:
                rows = all_rows
            energy_scan, wavelength, *_ = min(rows, key=__energy_offset)
            edge_position = __select_edge_position(
                wavelength,
                energy_scan.peakEnergy,
                energy_scan.inflectionEnergy,
            )
            res = {
                "energyscanid": energy_scan.energyScanId,
                "atom_type": energy_scan.element,
                "edge_position": edge_position,
            }
            if edge_position == "peak":
                res.update(
                    {
                        "fp": energy_scan.peakFPrime,
                        "fpp": energy_scan.peakFDoublePrime,
                    }
                )
            else:
                if edge_position == "infl":
                    res.update(
                        {
                            "fp": energy_scan.inflectionFPrime,
                            "fpp": energy_scan.inflectionFDoublePrime,
                        }
                    )
        except Exception:
            self.log.debug(
                "Matching energy scan data for dcid %s not available",
                dcid,
            )
            res = {}
        return res

    def get_protein_from_dcid(self, dcid: int, session: sqlalchemy.orm.session.Session):
        if protein := crud.get_protein_for_dcid(dcid, session):
            schema = isa.Protein.__marshmallow__(exclude=("externalId",))
            # XXX case sensitive? proteinid, proteintype
            return schema.dump(protein)

    def get_linked_pdb_files_for_dcid(
        self,
        dcid: int,
        session: sqlalchemy.orm.session.Session,
        pdb_tmpdir: pathlib.Path,
        user_pdb_dir: Optional[pathlib.Path] = None,
        ignore_pdb_codes: bool = False,
    ) -> list[dict]:
        """Get linked PDB files for a given data collection ID.

        Valid PDB codes will be returned as the code, PDB files will be copied into a
        unique subdirectory within the `pdb_tmpdir` directory. Optionally search for
        PDB files in the `user_pdb_dir` directory.
        """
        pdb_files = []
        for pdb in crud.get_pdb_for_dcid(dcid, session):
            if not ignore_pdb_codes and pdb.code is not None:
                pdb_code = pdb.code.strip()
                if pdb_code.isalnum() and len(pdb_code) == 4:
                    pdb_files.append(PDBFileOrCode(code=pdb_code, source=pdb.source))
                    continue
                elif pdb_code != "":
                    self.log.warning(
                        f"Invalid input PDB code '{pdb.code}' for pdbId {pdb.pdbId}"
                    )
            if pdb.contents not in ("", None):
                sha1 = hashlib.sha1(pdb.contents.encode()).hexdigest()
                assert pdb.name and "/" not in pdb.name, "Invalid PDB file name"
                pdb_dir = pdb_tmpdir / sha1
                pdb_dir.mkdir(parents=True, exist_ok=True)
                pdb_filepath = pdb_dir / pdb.name
                if not pdb_filepath.exists():
                    pdb_filepath.write_text(pdb.contents)
                pdb_files.append(
                    PDBFileOrCode(filepath=os.fspath(pdb_filepath), source=pdb.source)
                )

        if user_pdb_dir and user_pdb_dir.is_dir():
            # Look for matching .pdb files in user directory
            for f in user_pdb_dir.iterdir():
                if not f.stem or f.suffix != ".pdb" or not f.is_file():
                    continue
                self.log.info(f)
                pdb_files.append(PDBFileOrCode(filepath=os.fspath(f)))
        return [dataclasses.asdict(pdb) for pdb in pdb_files]

    def get_dcid_for_path(self, path, session: sqlalchemy.orm.session.Session):
        """Take a file path and try to identify a best match DCID"""
        if not path.startswith("/"):
            raise ValueError("Need absolute file path instead of %r" % path)
        extension = os.path.splitext(path)[1].lstrip(".")
        basepath, filename = os.path.split(path)
        basepath = basepath + "/"
        if extension:
            altpath = "__no_alternative__"
        else:
            altpath = path.rstrip("/") + "/"
        query = session.query(
            isa.DataCollection.dataCollectionId,
            isa.DataCollection.imageDirectory,
            isa.DataCollection.imagePrefix,
            isa.DataCollection.imageSuffix,
            isa.DataCollection.fileTemplate,
        ).filter(
            (isa.DataCollection.imageDirectory == basepath)
            | (isa.DataCollection.imageDirectory == altpath)
        )
        results = query.all()
        if extension:
            results = [r for r in results if r.imageSuffix == extension]
        if not results:
            raise ValueError("No matching DCID identified for %r" % path)

        if filename:
            candidates = [r for r in results if r.fileTemplate.startswith(filename)]
            if candidates:
                results = candidates
            candidates = [r for r in results if r.fileTemplate == filename]
            if candidates:
                results = candidates
            candidates = [r for r in results if filename.startswith(r[2])]
            if candidates:
                results = candidates
                prefix_lengths = [
                    len(os.path.commonprefix((r.fileTemplate, filename)))
                    for r in results
                ]
                max_prefix = max(prefix_lengths)
                candidates = [
                    r for r, pl in zip(results, prefix_lengths) if pl == max_prefix
                ]
                if candidates:
                    results = candidates

        if len(results) == 1:
            return results[0][0]

        raise ValueError(
            "Multiple matching candidates found:\n"
            + "\n".join("DCID %d %s%s" % (r[0], r[1], r[4]) for r in results)
        )

    def dc_info_to_filename_pattern(self, dc_info):
        template = _get(dc_info, "fileTemplate")
        if not template:
            return None
        if "#" not in template:
            return template
        template = template.replace("%", "%%")
        fmt = "%%0%dd" % template.count("#")
        prefix = template.split("#")[0]
        suffix = template.split("#")[-1]
        return prefix + fmt + suffix

    def dc_info_to_filename(self, dc_info, image_number=None):
        directory = dc_info["imageDirectory"]
        template = self.dc_info_to_filename_pattern(dc_info)
        if "%" not in template:
            return os.path.join(directory, template)
        if image_number:
            return os.path.join(directory, template % image_number)
        if dc_info["startImageNumber"]:
            return os.path.join(directory, template % dc_info["startImageNumber"])
        return None

    def dc_info_to_start_end(self, dc_info):
        start = _get(dc_info, "startImageNumber")
        number = _get(dc_info, "numberOfImages")
        if start is None or number is None:
            end = None
        else:
            end = start + number - 1
        return start, end

    def dc_info_is_grid_scan(self, dc_info):
        return bool(dc_info.get("gridinfo"))

    def dc_info_is_screening(self, dc_info):
        if dc_info.get("numberOfImages") is None:
            return None
        if dc_info["numberOfImages"] == 1:
            return True
        if dc_info["numberOfImages"] > 1 and dc_info["overlap"] != 0.0:
            return True
        return False

    def dc_info_is_rotation_scan(self, dc_info):
        overlap = _get(dc_info, "overlap")
        axis_range = _get(dc_info, "axisRange")
        if overlap is None or axis_range is None:
            return None
        return overlap == 0.0 and axis_range > 0

    def classify_dc(self, dc_info, experiment_type: str | None):
        return {
            "grid": self.dc_info_is_grid_scan(dc_info),
            "screen": self.dc_info_is_screening(dc_info)
            and not (
                experiment_type == "Serial Fixed" or experiment_type == "Serial Jet"
            ),
            "rotation": self.dc_info_is_rotation_scan(dc_info),
            "serial_fixed": experiment_type == "Serial Fixed",
            "serial_jet": experiment_type == "Serial Jet",
            "diamond_anvil_cell": experiment_type == "Diamond Anvil High Pressure",
        }

    @staticmethod
    def get_visit_directory_from_image_directory(directory):
        """/dls/${beamline}/data/${year}/${visit}/...
        -> /dls/${beamline}/data/${year}/${visit}"""
        if not directory:
            return None
        visit_base = re_visit_base.search(directory)
        if not visit_base:
            return None
        return visit_base.group(1)

    @staticmethod
    def get_visit_from_image_directory(directory):
        """/dls/${beamline}/data/${year}/${visit}/...
        -> ${visit}"""
        if not directory:
            return None
        visit_base = re_visit_base.search(directory)
        if not visit_base:
            return None
        return visit_base.group(2)

    def dc_info_to_working_directory(self, dc_info):
        directory = dc_info.get("imageDirectory")
        if not directory:
            return None
        visit = self.get_visit_directory_from_image_directory(directory)
        rest = directory[len(visit) + 1 :]

        collection_path = dc_info["imagePrefix"] or ""
        dc_number = dc_info["dataCollectionNumber"] or ""
        if collection_path or dc_number:
            collection_path = f"{collection_path}_{dc_number}"
        return os.path.join(
            visit, "tmp", "zocalo", rest, collection_path, dc_info["uuid"]
        )

    def dc_info_to_results_directory(self, dc_info):
        directory = dc_info.get("imageDirectory")
        if not directory:
            return None
        visit = self.get_visit_directory_from_image_directory(directory)
        rest = directory[len(visit) + 1 :]

        collection_path = dc_info["imagePrefix"] or ""
        dc_number = dc_info["dataCollectionNumber"] or ""
        if collection_path or dc_number:
            collection_path = f"{collection_path}_{dc_number}"
        return os.path.join(visit, "processed", rest, collection_path, dc_info["uuid"])

    def get_diffractionplan_from_dcid(
        self, dcid: int, session: sqlalchemy.orm.session.Session
    ):
        if dp := crud.get_diffraction_plan_for_dcid(dcid, session):
            # XXX case sensitive?
            schema = isa.DiffractionPlan.__marshmallow__()
            return schema.dump(dp)

    def get_priority_processing_for_dc_info(
        self, sample_id: int, session: sqlalchemy.orm.session.Session
    ):
        return crud.get_priority_processing_for_sample_id(sample_id, session)


def ready_for_processing(
    message, parameters, session: sqlalchemy.orm.session.Session | None = None
):
    """Check whether this message is ready for templatization."""

    if session is None:
        session = Session()

    if not parameters.get("ispyb_wait_for_runstatus"):
        return True

    dcid = parameters.get("ispyb_dcid")
    if not dcid:
        return True

    return crud.get_run_status_for_dcid(dcid, session) is not None


def ispyb_filter(
    message,
    parameters,
    session: sqlalchemy.orm.session.Session | None = None,
    io_timeout: float = 10,
):
    """Do something to work out what to do with this data..."""

    if session is None:
        session = Session()

    i = ispybtbx()

    message, parameters = i(message, parameters, session)

    if "ispyb_dcid" not in parameters:
        return message, parameters

    # FIXME put in here logic to check input if set i.e. if dc_id==0 then check
    # files exist; if image already set check they exist, ...

    dc_id = parameters["ispyb_dcid"]
    dc_info = i.get_dc_info(dc_id, session)
    dcg_id: int | None = dc_info.get("dataCollectionGroupId")

    if not dc_info:
        raise ValueError(f"No database entry found for dcid={dc_id}: {dc_id}")
    dc_info["uuid"] = parameters.get("guid") or str(uuid.uuid4())
    parameters["ispyb_beamline"] = i.get_beamline_from_dcid(dc_id, session)
    parameters["ispyb_preferred_datacentre"] = "cs05r"
    parameters["ispyb_preferred_scheduler"] = "slurm"
    parameters["ispyb_preferred_queue_variant"] = ".cs05r_gpfs"

    parameters["ispyb_detectorclass"] = i.dc_info_to_detectorclass(dc_info, session)
    parameters["ispyb_dc_info"] = dc_info
    parameters["ispyb_dc_info"]["gridinfo"] = i.get_gridscan_info(
        dc_info.get("dataCollectionId"), dc_info.get("dataCollectionGroupId"), session
    )
    parameters["ispyb_dcg_experiment_type"] = i.get_dcg_experiment_type(
        dc_info.get("dataCollectionGroupId"), session
    )
    dc_class = i.classify_dc(dc_info, parameters["ispyb_dcg_experiment_type"])
    parameters["ispyb_dc_class"] = dc_class
    diff_plan_info = i.get_diffractionplan_from_dcid(dc_id, session)
    parameters["ispyb_diffraction_plan"] = diff_plan_info
    protein_info = i.get_protein_from_dcid(dc_id, session)
    parameters["ispyb_protein_info"] = protein_info
    energy_scan_info = i.get_energy_scan_from_dcid(dc_id, session)
    parameters["ispyb_energy_scan_info"] = energy_scan_info
    start, end = i.dc_info_to_start_end(dc_info)
    sample_id = parameters["ispyb_dc_info"].get("BLSAMPLEID")
    priority_processing = crud.get_priority_processing_for_sample_id(sample_id, session)
    if not priority_processing:
        priority_processing = "xia2/DIALS"
    parameters["ispyb_preferred_processing"] = priority_processing
    parameters["ispyb_image_first"] = start
    parameters["ispyb_image_last"] = end
    parameters["ispyb_image_template"] = dc_info.get("fileTemplate")
    parameters["ispyb_image_directory"] = dc_info.get("imageDirectory")
    parameters["ispyb_image_pattern"] = i.dc_info_to_filename_pattern(dc_info)
    if not parameters.get("ispyb_image") and start is not None and end is not None:
        parameters["ispyb_image"] = "%s:%d:%d" % (
            i.dc_info_to_filename(dc_info),
            start,
            end,
        )
    parameters["ispyb_visit"] = i.get_visit_from_image_directory(
        dc_info.get("imageDirectory")
    )
    visit_directory = i.get_visit_directory_from_image_directory(
        dc_info.get("imageDirectory")
    )
    parameters["ispyb_visit_directory"] = visit_directory
    parameters["ispyb_working_directory"] = i.dc_info_to_working_directory(dc_info)
    parameters["ispyb_results_directory"] = i.dc_info_to_results_directory(dc_info)
    parameters["ispyb_space_group"] = ""
    parameters["ispyb_related_sweeps"] = []
    parameters["ispyb_reference_geometry"] = None
    if visit_directory:
        parameters["ispyb_pdb"] = i.get_linked_pdb_files_for_dcid(
            dc_id,
            session,
            pdb_tmpdir=pathlib.Path(visit_directory) / "tmp" / "pdb",
            user_pdb_dir=pathlib.Path(visit_directory) / "processing" / "pdb",
        )
        reference_geometry = (
            pathlib.Path(visit_directory) / "processing" / "reference_geometry.expt"
        )
        if reference_geometry.exists():
            parameters["ispyb_reference_geometry"] = os.fspath(reference_geometry)

    parameters["ispyb_project"] = (
        parameters.get("ispyb_visit") or "AUTOMATIC"
    ).replace("-", "v")
    if parameters["ispyb_dc_info"].get("imagePrefix") and parameters[
        "ispyb_dc_info"
    ].get("dataCollectionNumber"):
        parameters["ispyb_crystal"] = "x" + re.sub(
            "[^A-Za-z0-9]+",
            "",
            parameters["ispyb_dc_info"]["imagePrefix"]
            + str(parameters["ispyb_dc_info"]["dataCollectionNumber"]),
        )
    else:
        parameters["ispyb_crystal"] = "DEFAULT"

    space_group, cell = i.get_space_group_and_unit_cell(dc_id, session)
    if not any((space_group, cell)) and visit_directory:
        space_group, cell = i.get_space_group_and_unit_cell_from_yaml(
            parameters, io_timeout=io_timeout
        )
    parameters["ispyb_space_group"] = space_group
    parameters["ispyb_unit_cell"] = cell

    # related dcids via sample groups
    parameters["ispyb_related_dcids"] = i.get_sample_group_dcids(
        parameters, session, io_timeout=io_timeout
    )
    related_dcids = None
    if parameters["ispyb_dc_info"].get("BLSAMPLEID"):
        # if a sample is linked to the dc, then get dcids on the same sample
        sample_id = parameters["ispyb_dc_info"].get("BLSAMPLEID")
        related_dcids = i.get_sample_dcids(sample_id, session)
    elif dcid := parameters.get("ispyb_dcid"):
        # else get dcids collected into the same image directory
        related_dcids = i.get_related_dcids_same_directory(dcid, session)
    if related_dcids:
        parameters["ispyb_related_dcids"].append(related_dcids)
    logger.debug(f"ispyb_related_dcids: {parameters['ispyb_related_dcids']}")
    parameters["ispyb_dcg_dcids"] = i.get_dcg_dcids(
        dc_info.get("dataCollectionId"), dc_info.get("dataCollectionGroupId"), session
    )

    if (
        "ispyb_processing_job" in parameters
        and parameters["ispyb_processing_job"]["recipe"]
        and not message.get("recipes")
        and not message.get("custom_recipe")
    ):
        # Prefix recipe name coming from ispyb/synchweb with 'ispyb-'
        message["recipes"] = ["ispyb-" + parameters["ispyb_processing_job"]["recipe"]]
        return message, parameters

    if dc_class["grid"]:
        return message, parameters

    if dc_class["screen"]:
        parameters["ispyb_images"] = ""
        return message, parameters

    if not (dc_class["rotation"] or dc_class["serial_fixed"] or dc_class["serial_jet"]):
        # possibly EM dataset
        return message, parameters

    schema = isa.Event.__marshmallow__()
    ispyb_events = crud.get_ssx_events_for_dcid(dc_id, session)
    events = [{"eventType": e.EventType.name, **schema.dump(e)} for e in ispyb_events]
    shots_per_image = int(
        sum(e["repetition"] for e in events if e["eventType"] == "XrayDetection")
    )
    parameters["ispyb_ssx_events"] = events or None
    parameters["ispyb_ssx_shots_per_image"] = shots_per_image or None

    # for the moment we do not want multi-xia2 for /dls/mx i.e. VMXi
    # beware if other projects start using this directory structure will
    # need to be smarter here...

    # Handle related DCID properties via DataCollectionGroup, if there is one
    if dcg_id:
        stmt = select(
            models.DataCollection.dataCollectionId,
            models.DataCollection.startImageNumber,
            models.DataCollection.numberOfImages,
            models.DataCollection.overlap,
            models.DataCollection.axisRange,
            models.DataCollection.fileTemplate,
            models.DataCollection.imageDirectory,
        ).where(models.DataCollection.dataCollectionGroupId == dcg_id)

        related_images = []

        for dc in session.execute(stmt).all():
            start, end = i.dc_info_to_start_end(dc)
            parameters["ispyb_related_sweeps"].append((dc.dataCollectionId, start, end))

            # We don't get related images for /dls/mx collections
            if (
                not parameters["ispyb_image_directory"].startswith("/dls/mx")
                and dc.dataCollectionId != dc_id
                and i.dc_info_is_rotation_scan(dc)
            ):
                related_images.append(
                    "%s:%d:%d" % (i.dc_info_to_filename(dc), start, end)
                )

        if not parameters.get("ispyb_images"):
            parameters["ispyb_images"] = ",".join(related_images)

    return message, parameters


def load_configuration_file(ispyb_info):
    visit_dir = pathlib.Path(ispyb_info["ispyb_visit_directory"])
    processing_dir = visit_dir / "processing"

    for f in list(processing_dir.glob("*.yml")) + list(processing_dir.glob("*.yaml")):
        prefix = f.stem.lower()
        image_path = os.path.join(
            ispyb_info["ispyb_image_directory"], ispyb_info["ispyb_image_template"]
        )
        if prefix in os.path.relpath(image_path, visit_dir).lower().split(os.sep):
            with open(f) as fh:
                try:
                    return yaml.safe_load(fh)
                except yaml.YAMLError as exc:
                    logger.warning(
                        "Error in configuration file %s:\n%s", f, exc, exc_info=True
                    )


def load_sample_group_config_file(ispyb_info):
    if any(
        not ispyb_info.get(entry)
        for entry in (
            "ispyb_visit_directory",
            "ispyb_image_directory",
            "ispyb_image_template",
        )
    ):
        return
    visit_dir = ispyb_info["ispyb_visit_directory"]
    processing_dir = os.path.join(ispyb_info["ispyb_visit_directory"], "processing")
    config_file = os.path.join(processing_dir, "sample_groups.yml")
    image_path = os.path.join(
        ispyb_info["ispyb_image_directory"], ispyb_info["ispyb_image_template"]
    )
    if os.path.isfile(config_file):
        with open(config_file) as fh:
            try:
                sample_groups = yaml.safe_load(fh)
            except yaml.YAMLError as exc:
                logger.warning(
                    f"Error in configuration file {config_file}:\n{exc}",
                    exc_info=True,
                )
            else:
                groups = []
                for group in sample_groups:
                    for prefix in group:
                        if prefix in os.path.relpath(image_path, visit_dir).split(
                            os.sep
                        ):
                            groups.append(group)
                return groups
    else:
        logger.debug(
            f"Config file {config_file} either does not exist or is not a file"
        )


def work(dc_ids):
    import pprint

    pp = pprint.PrettyPrinter(indent=2)

    with Session() as session:
        for dc_id in dc_ids:
            message = {}
            parameters = {"ispyb_dcid": dc_id}
            message, parameters = ispyb_filter(message, parameters, session)

            pp.pprint("Message:")
            pp.pprint(message)
            pp.pprint("Parameters:")
            pp.pprint(parameters)


if __name__ == "__main__":
    import sys

    if len(sys.argv) == 1:
        raise RuntimeError("for this mode of testing pass list of DCID on CL")
    else:
        work(map(int, sys.argv[1:]))
