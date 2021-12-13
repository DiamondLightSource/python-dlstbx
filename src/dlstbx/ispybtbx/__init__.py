from __future__ import annotations

import decimal
import glob
import itertools
import logging
import os
import re
import uuid
from typing import Optional, Tuple, Union

import ispyb.sqlalchemy as isa
import marshmallow.fields
import sqlalchemy
import yaml
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from sqlalchemy.orm import Load, aliased, joinedload, selectinload, sessionmaker

logger = logging.getLogger("dlstbx.ispybtbx")

_gpfs03_beamlines = {
    "b07",
    "b07-1",
    "b18",
    "b21",
    "b22",
    "i05",
    "i05-1",
    "i07",
    "i08",
    "i08-1",
    "i09",
    "i09-1",
    "i09-2",
    "i10",
    "i10-1",
    "i12",
    "i13",
    "i13-1",
    "i14",
    "i14-1",
    "i19",
    "i19-1",
    "i19-2",
    "i20",
    "i20-1",
    "i21",
    "k11",
    "p99",
}


Session = sessionmaker(
    bind=sqlalchemy.create_engine(isa.url(), connect_args={"use_pure": True})
)


def setup_marshmallow_schema():
    with Session() as session:
        # https://marshmallow-sqlalchemy.readthedocs.io/en/latest/recipes.html#automatically-generating-schemas-for-sqlalchemy-models
        for class_ in isa.Base.registry._class_registry.values():
            if hasattr(class_, "__tablename__"):

                class Meta(object):
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
        setup_marshmallow_schema()
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

    def get_gridscan_info(self, dc_info, session: sqlalchemy.orm.session.Session):
        """Extract GridInfo table contents for a DC group ID."""
        dcid = dc_info.get("dataCollectionId")
        dcgid = dc_info.get("dataCollectionGroupId")
        query = session.query(isa.GridInfo).filter(
            (isa.GridInfo.dataCollectionId == dcid)
            | (isa.GridInfo.dataCollectionGroupId == dcgid)
        )
        gridinfo = query.first()
        if not gridinfo:
            return {}
        schema = isa.GridInfo.__marshmallow__()
        return schema.dump(gridinfo)

    def get_dc_info(self, dc_id, session: sqlalchemy.orm.session.Session):
        query = session.query(isa.DataCollection).filter(
            isa.DataCollection.dataCollectionId == dc_id
        )
        dc = query.first()
        if dc is None:
            return {}
        schema = isa.DataCollection.__marshmallow__()
        return schema.dump(dc)

    def get_beamline_from_dcid(self, dc_id, session: sqlalchemy.orm.session.Session):
        query = (
            session.query(isa.BLSession)
            .join(
                isa.DataCollection,
                isa.DataCollection.SESSIONID == isa.BLSession.sessionId,
            )
            .filter(isa.DataCollection.dataCollectionId == dc_id)
        )
        bs = query.first()
        if bs:
            return bs.beamLineName

    def dc_info_to_detectorclass(
        self, dc_info, session: sqlalchemy.orm.session.Session
    ):
        dcid = dc_info.get("dataCollectionId")
        if not dcid:
            return None
        query = (
            session.query(isa.DataCollection)
            .filter_by(dataCollectionId=dcid)
            .options(
                Load(isa.DataCollection).load_only("fileTemplate"),
                joinedload(isa.DataCollection.Detector),
            )
        )
        dc = query.first()
        if dc and dc.Detector:
            if dc.Detector.detectorModel.lower().startswith("eiger"):
                return "eiger"
            elif dc.Detector.detectorModel.lower().startswith("pilatus"):
                return "pilatus"

        # Fallback on examining the file extension if nothing recorded in ISPyB
        template = dc.fileTemplate
        if not template:
            return None
        if template.endswith("master.h5"):
            return "eiger"
        elif template.endswith(".cbf"):
            return "pilatus"

    def get_related_dcs(self, group, session: sqlalchemy.orm.session.Session):
        query = (
            session.query(isa.DataCollection.dataCollectionId)
            .join(isa.DataCollectionGroup)
            .filter(isa.DataCollectionGroup.dataCollectionGroupId == group)
        )
        return list(itertools.chain.from_iterable(query.all()))

    def get_sample_group_dcids(
        self, ispyb_info, session: sqlalchemy.orm.session.Session
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
                sample_groups = load_sample_group_config_file(ispyb_info)
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

    def get_sample_dcids(self, ispyb_info, session: sqlalchemy.orm.session.Session):
        dcid = ispyb_info.get("ispyb_dcid")
        sample_id = ispyb_info["ispyb_dc_info"].get("BLSAMPLEID")
        if not dcid or not sample_id:
            return None

        this_sample = aliased(isa.BLSample, name="this_sample")
        other_sample = aliased(isa.BLSample)
        query = (
            session.query(this_sample, isa.DataCollection.dataCollectionId)
            .join(
                other_sample,
                other_sample.blSampleId == this_sample.blSampleId,
            )
            .join(
                isa.DataCollection,
                isa.DataCollection.BLSAMPLEID == other_sample.blSampleId,
            )
            .filter(other_sample.blSampleId == sample_id)
        )
        results = query.all()
        if results:
            sample = results[0].this_sample
            related_dcids = {
                "dcids": [row.dataCollectionId for row in results],
                "sample_id": sample.blSampleId,
                "name": sample.name,
            }
            logger.debug(f"dcids defined via BLSample for dcid={dcid}: {related_dcids}")
            return related_dcids

    def get_related_dcids_same_directory(
        self, ispyb_info, session: sqlalchemy.orm.session.Session
    ):
        dcid = ispyb_info.get("ispyb_dcid")
        if not dcid:
            return None

        dc1 = aliased(isa.DataCollection)
        dc2 = aliased(isa.DataCollection)
        query = (
            session.query(dc2.dataCollectionId)
            .join(
                dc1,
                (dc1.imageDirectory == dc2.imageDirectory)
                & (dc1.dataCollectionId != dc2.dataCollectionId)
                & (dc1.imageDirectory is not None),
            )
            .filter(dc1.dataCollectionId == dcid)
        )
        return {"dcids": list(itertools.chain.from_iterable(query.all()))}

    def get_dcg_dcids(self, dc_info, session: sqlalchemy.orm.session.Session):
        dcid = dc_info.get("dataCollectionId")
        dcgid = dc_info.get("dataCollectionGroupId")
        if not dcgid:
            return
        query = session.query(isa.DataCollection.dataCollectionId).filter(
            isa.DataCollection.dataCollectionGroupId == dcgid,
            isa.DataCollection.dataCollectionId != dcid,
        )
        return list(itertools.chain.from_iterable(query.all()))

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
        self, dcid, session: sqlalchemy.orm.session.Session
    ):
        query = (
            session.query(isa.Crystal)
            .join(isa.BLSample)
            .join(
                isa.DataCollection,
                isa.DataCollection.BLSAMPLEID == isa.BLSample.blSampleId,
            )
            .filter(isa.DataCollection.dataCollectionId == dcid)
        )
        c = query.first()
        if not c or not c.spaceGroup:
            return "", False
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

    def get_protein_from_dcid(self, dcid, session: sqlalchemy.orm.session.Session):
        query = (
            session.query(isa.Protein)
            .join(isa.Crystal)
            .join(isa.BLSample)
            .join(
                isa.DataCollection,
                isa.DataCollection.BLSAMPLEID == isa.BLSample.blSampleId,
            )
            .filter(isa.DataCollection.dataCollectionId == dcid)
        )
        protein = query.first()
        if protein:
            schema = isa.Protein.__marshmallow__(exclude=("externalId",))
            # XXX case sensitive? proteinid, proteintype
            return schema.dump(protein)

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
        template = dc_info.get("fileTemplate")
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
        start = dc_info.get("startImageNumber")
        number = dc_info.get("numberOfImages")
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
        overlap = dc_info.get("overlap")
        axis_range = dc_info.get("axisRange")
        if overlap is None or axis_range is None:
            return None
        return overlap == 0.0 and axis_range > 0

    def classify_dc(self, dc_info):
        return {
            "grid": self.dc_info_is_grid_scan(dc_info),
            "screen": self.dc_info_is_screening(dc_info),
            "rotation": self.dc_info_is_rotation_scan(dc_info),
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
        self, dcid, session: sqlalchemy.orm.session.Session
    ):
        query = (
            session.query(isa.DiffractionPlan)
            .join(isa.BLSample)
            .join(
                isa.DataCollection,
                isa.DataCollection.BLSAMPLEID == isa.BLSample.blSampleId,
            )
            .filter(isa.DataCollection.dataCollectionId == dcid)
        )
        dp = query.first()
        if dp:
            # XXX case sensitive?
            schema = isa.DiffractionPlan.__marshmallow__()
            return schema.dump(dp)

    def get_priority_processing_for_dc_info(
        self, dc_info, session: sqlalchemy.orm.session.Session
    ):
        blsampleid = dc_info.get("BLSAMPLEID")
        if not blsampleid:
            return None
        query = (
            session.query(isa.ProcessingPipeline.name)
            .join(isa.Container)
            .join(isa.BLSample)
            .filter(isa.BLSample.blSampleId == blsampleid)
        )
        pipeline = query.first()
        if pipeline:
            return pipeline.name


def ready_for_processing(message, parameters, session: sqlalchemy.orm.session.Session):
    """Check whether this message is ready for templatization."""
    if not parameters.get("ispyb_wait_for_runstatus"):
        return True

    dcid = parameters.get("ispyb_dcid")
    if not dcid:
        return True

    query = session.query(isa.DataCollection.runStatus).filter(
        isa.DataCollection.dataCollectionId == dcid
    )
    return query.one().runStatus is not None


def ispyb_filter(message, parameters, session: sqlalchemy.orm.session.Session):
    """Do something to work out what to do with this data..."""

    i = ispybtbx()

    message, parameters = i(message, parameters, session)

    if "ispyb_dcid" not in parameters:
        return message, parameters

    # FIXME put in here logic to check input if set i.e. if dc_id==0 then check
    # files exist; if image already set check they exist, ...

    dc_id = parameters["ispyb_dcid"]

    dc_info = i.get_dc_info(dc_id, session)
    dc_info["uuid"] = parameters.get("guid") or str(uuid.uuid4())
    parameters["ispyb_beamline"] = i.get_beamline_from_dcid(dc_id, session)
    if str(parameters["ispyb_beamline"]).lower() in _gpfs03_beamlines:
        parameters["ispyb_preferred_datacentre"] = "hamilton"
    else:
        parameters["ispyb_preferred_datacentre"] = "cluster"
    parameters["ispyb_detectorclass"] = i.dc_info_to_detectorclass(dc_info, session)
    parameters["ispyb_dc_info"] = dc_info
    parameters["ispyb_dc_info"]["gridinfo"] = i.get_gridscan_info(dc_info, session)
    parameters["ispyb_dcg_experiment_type"] = i.get_dcg_experiment_type(
        dc_info.get("dataCollectionGroupId"), session
    )
    dc_class = i.classify_dc(dc_info)
    parameters["ispyb_dc_class"] = dc_class
    diff_plan_info = i.get_diffractionplan_from_dcid(dc_id, session)
    parameters["ispyb_diffraction_plan"] = diff_plan_info
    protein_info = i.get_protein_from_dcid(dc_id, session)
    parameters["ispyb_protein_info"] = protein_info
    energy_scan_info = i.get_energy_scan_from_dcid(dc_id, session)
    parameters["ispyb_energy_scan_info"] = energy_scan_info
    start, end = i.dc_info_to_start_end(dc_info)
    priority_processing = i.get_priority_processing_for_dc_info(dc_info, session)
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
    parameters["ispyb_visit_directory"] = i.get_visit_directory_from_image_directory(
        dc_info.get("imageDirectory")
    )
    parameters["ispyb_working_directory"] = i.dc_info_to_working_directory(dc_info)
    parameters["ispyb_results_directory"] = i.dc_info_to_results_directory(dc_info)
    parameters["ispyb_space_group"] = ""
    parameters["ispyb_related_sweeps"] = []

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
    if not any((space_group, cell)):
        try:
            params = load_configuration_file(parameters)
        except Exception as exc:
            logger.warning(
                f"Error loading configuration file for dcid={dc_id}:\n{exc}",
                exc_info=True,
            )
        else:
            if params:
                space_group = params.get("ispyb_space_group")
                cell = params.get("ispyb_unit_cell")
                if isinstance(cell, str):
                    try:
                        cell = [float(p) for p in cell.replace(",", " ").split()]
                    except ValueError:
                        logger.warning(
                            "Can't interpret unit cell: %s (dcid: %s)", str(cell), dc_id
                        )
                        cell = None
    parameters["ispyb_space_group"] = space_group
    parameters["ispyb_unit_cell"] = cell

    # related dcids via sample groups
    parameters["ispyb_related_dcids"] = i.get_sample_group_dcids(parameters, session)
    if parameters["ispyb_dc_info"].get("BLSAMPLEID"):
        # if a sample is linked to the dc, then get dcids on the same sample
        related_dcids = i.get_sample_dcids(parameters, session)
    else:
        # else get dcids collected into the same image directory
        related_dcids = i.get_related_dcids_same_directory(parameters, session)
    if related_dcids:
        parameters["ispyb_related_dcids"].append(related_dcids)
    logger.debug(f"ispyb_related_dcids: {parameters['ispyb_related_dcids']}")
    parameters["ispyb_dcg_dcids"] = i.get_dcg_dcids(dc_info, session)

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

    if not dc_class["rotation"]:
        # possibly EM dataset
        return message, parameters

    # for the moment we do not want multi-xia2 for /dls/mx i.e. VMXi
    # beware if other projects start using this directory structure will
    # need to be smarter here...

    if dc_info["dataCollectionGroupId"]:
        related_dcs = i.get_related_dcs(dc_info["dataCollectionGroupId"], session)
        if parameters["ispyb_image_directory"].startswith("/dls/mx"):
            related = []
        else:
            related = list(sorted(set(related_dcs)))
        for dc in related_dcs:
            info = i.get_dc_info(dc, session)
            start, end = i.dc_info_to_start_end(info)
            parameters["ispyb_related_sweeps"].append((dc, start, end))

    related_images = []

    if not parameters.get("ispyb_images"):
        # may have been set via __call__ for reprocessing jobs
        parameters["ispyb_images"] = ""
        for dc in related:

            # FIXME logic: should this exclude dc > dc_id?
            if dc == dc_id:
                continue

            info = i.get_dc_info(dc, session)
            other_dc_class = i.classify_dc(info)
            if other_dc_class["rotation"]:
                start, end = i.dc_info_to_start_end(info)

                related_images.append(
                    "%s:%d:%d" % (i.dc_info_to_filename(info), start, end)
                )

            parameters["ispyb_images"] = ",".join(related_images)

    return message, parameters


def load_configuration_file(ispyb_info):
    visit_dir = ispyb_info["ispyb_visit_directory"]
    processing_dir = os.path.join(visit_dir, "processing")
    for f in glob.glob(os.path.join(processing_dir, "*.yml")):
        prefix = os.path.splitext(os.path.basename(f))[0]
        image_path = os.path.join(
            ispyb_info["ispyb_image_directory"], ispyb_info["ispyb_image_template"]
        )
        if prefix in os.path.relpath(image_path, visit_dir):
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
