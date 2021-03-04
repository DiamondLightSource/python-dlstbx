import glob
import logging
import os
import re
import uuid
import yaml

import ispyb
import ispyb.sqlalchemy
import mysql.connector  # installed by ispyb
import sqlalchemy.orm
from ispyb.sqlalchemy import DataCollection


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


Session = sqlalchemy.orm.sessionmaker(
    bind=sqlalchemy.create_engine(
        ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
    )
)


def setup_marshmallow_schema():
    from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

    with Session() as session:
        # https://marshmallow-sqlalchemy.readthedocs.io/en/latest/recipes.html#automatically-generating-schemas-for-sqlalchemy-models
        for class_ in ispyb.sqlalchemy.Base.registry._class_registry.values():
            if hasattr(class_, "__tablename__"):

                class Meta(object):
                    model = class_
                    sqla_session = session
                    load_instance = True
                    include_fk = True

                schema_class_name = "%sSchema" % class_.__name__
                schema_class = type(
                    schema_class_name, (SQLAlchemyAutoSchema,), {"Meta": Meta}
                )
                setattr(class_, "__marshmallow__", schema_class)


def _ispyb_api():
    if not hasattr(_ispyb_api, "instance"):
        setattr(
            _ispyb_api,
            "instance",
            ispyb.open("/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg"),
        )
    return _ispyb_api.instance


future_enabled = False
re_visit_base = re.compile(r"^(.*\/([a-z][a-z][0-9]+-[0-9]+))\/")


def _enable_future():
    global future_enabled
    if future_enabled:
        return
    import ispyb.model.__future__

    ispyb.model.__future__.enable("/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg")
    future_enabled = True


class ispybtbx:
    def __init__(self):
        self.legacy_init()

        self.log = logging.getLogger("dlstbx.ispybtbx")
        self.log.debug("ISPyB objects set up")

    def __call__(self, message, parameters):
        reprocessing_id = parameters.get(
            "ispyb_reprocessing_id", parameters.get("ispyb_process")
        )
        if reprocessing_id:
            parameters["ispyb_process"] = reprocessing_id
            try:
                rp = _ispyb_api().get_processing_job(reprocessing_id)
                parameters["ispyb_images"] = ",".join(
                    "%s:%d:%d"
                    % (
                        sweep.data_collection.file_template_full_python % sweep.start
                        if "%" in sweep.data_collection.file_template_full_python
                        else sweep.data_collection.file_template_full_python,
                        sweep.start,
                        sweep.end,
                    )
                    for sweep in rp.sweeps
                )
                # ispyb_reprocessing_parameters is the deprecated method of
                # accessing the processing parameters
                parameters["ispyb_reprocessing_parameters"] = {
                    k: v.value for k, v in dict(rp.parameters).items()
                }
                # ispyb_processing_parameters is the preferred method of
                # accessing the processing parameters
                processing_parameters = {}
                for k, v in rp.parameters:
                    processing_parameters.setdefault(k, [])
                    processing_parameters[k].append(v.value)
                parameters["ispyb_processing_parameters"] = processing_parameters
            except ispyb.NoResult:
                self.log.warning("Reprocessing ID %s not found", str(reprocessing_id))
        return message, parameters

    def get_gridscan_info(self, dcgid):
        """Extract GridInfo table contents for a DC group ID."""
        newgrid = _ispyb_api().get_data_collection_group(dcgid).gridinfo
        if not newgrid:
            return {}  # This is no grid scan.
        return {
            "steps_x": newgrid.steps_x,
            "steps_y": newgrid.steps_y,
            "dx_mm": newgrid.dx_mm,
            "dy_mm": newgrid.dy_mm,
            "orientation": newgrid.orientation,
            "snaked": newgrid.snaked,
            "snapshot_offsetXPixel": newgrid.snapshot_offset_pixel_x,
            "snapshot_offsetYPixel": newgrid.snapshot_offset_pixel_y,
            #       'recordTimeStamp': newgrid.timestamp,
            "gridInfoId": newgrid.id,
            "pixelsPerMicronX": newgrid.pixels_per_micron_x,
            "pixelsPerMicronY": newgrid.pixels_per_micron_y,
            "dataCollectionGroupId": newgrid.dcgid,
        }

    def legacy_init(self):
        # Temporary API to ISPyB while I wait for a proper one using stored procedures
        # - beware here be dragons, written by a hacker who is not a database wonk.

        from configparser import ConfigParser

        ispyb_config = ConfigParser()
        ispyb_config.read("/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg")
        secret_ingredients = ispyb_config["ispyb"]

        self.conn = mysql.connector.connect(
            host=secret_ingredients["host"],
            port=secret_ingredients["port"],
            user=secret_ingredients["username"],
            password=secret_ingredients["password"],
            database=secret_ingredients["database"],
            use_pure=True,
        )

        # gather information on tables so we can map the data structures
        # back to named tuples / dictionaries in the results
        tables = ["DataCollection", "DataCollectionGroup"]
        self.columns = {}
        cursor = self.conn.cursor()
        for table in tables:
            query = "describe %s;" % table
            cursor.execute(query)
            columns = []
            for record in cursor:
                name = record[0]
                columns.append(name)
            self.columns[table] = columns

        self._cursor = self.conn.cursor()
        setup_marshmallow_schema()

    def __del__(self):
        if hasattr(self, "conn") and self.conn:
            self.conn.close()

    def execute(self, query, parameters=None):
        cursor = self._cursor
        if parameters:
            if isinstance(parameters, (str, int)):
                parameters = (parameters,)
            cursor.execute(query, parameters)
        else:
            cursor.execute(query)
        results = [result for result in cursor]
        return results

    def commit(self):
        self.conn.commit()

    def get_dc_info(self, dc_id):
        with Session() as session:
            query = session.query(DataCollection).filter(
                DataCollection.dataCollectionId == dc_id
            )
            dc = query.first()
            if dc is None:
                return {}
            schema = DataCollection.__marshmallow__()
            return schema.dump(dc)

    def get_beamline_from_dcid(self, dc_id):
        results = self.execute(
            "SELECT bs.beamlineName FROM BLSession bs INNER JOIN DataCollectionGroup dcg ON dcg.sessionId = bs.sessionId INNER JOIN DataCollection dc ON dc.dataCollectionGroupId = dcg.dataCollectionGroupId WHERE dc.dataCollectionId = %s;"
            % str(dc_id)
        )
        if not results:
            return None
        assert len(results) == 1
        result = results[0][0]
        return result

    def dc_info_to_detectorclass(self, dc_info):
        dcid = dc_info.get("dataCollectionId")
        if not dcid:
            return None
        _enable_future()
        try:
            detector = _ispyb_api().get_data_collection(dcid).detector
        except mysql.connector.errors.ProgrammingError:
            pass
        else:
            # Currently get a database table permission error:
            #   SELECT command denied to user 'ispyb_scripts' for table 'Detector'
            if detector and detector.model.lower().startswith("eiger"):
                return "eiger"
            elif detector and detector.model.lower().startswith("pilatus"):
                return "pilatus"

        # Fallback on examining the file extension if nothing recorded in ISPyB
        template = dc_info.get("fileTemplate")
        if not template:
            return None
        if template.endswith("master.h5"):
            return "eiger"
        elif template.endswith(".cbf"):
            return "pilatus"

    def get_related_dcs(self, group):
        matches = self.execute(
            "select datacollectionid from DataCollection "
            "where dataCollectionGroupId=%s;",
            group,
        )
        assert len(matches) >= 1
        dc_ids = [m[0] for m in matches]
        return dc_ids

    def get_sample_group_dcids(self, ispyb_info):
        dcid = ispyb_info.get("ispyb_dcid")
        if not dcid:
            return []

        # First attempt to get sample group definitions from BLSampleGroup via
        # ispyb-api lookup (depends on DiamondLightSource/ispyb-api#104)
        _enable_future()
        related_dcids = []
        try:
            sample_groups = _ispyb_api().get_data_collection(dcid).sample_groups
        except mysql.connector.errors.ProgrammingError as e:
            logger.debug(
                f"Error looking up sample_groups for dcid={dcid}:\n{e}",
                exc_info=True,
            )
        except AttributeError as e:
            logger.debug(
                f"sample_groups not yet supported by ispyb-api version:\n{e}",
                exc_info=True,
            )
        else:
            for sample_group in sample_groups:
                sample_group.load()
                related_dcids.append(
                    {
                        "dcids": sample_group.dcids,
                        "sample_group_id": sample_group.id,
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
        print(related_dcids)
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
                    for sample_group in sample_groups:
                        sample_group_dcids = []
                        sessionid = self.get_bl_sessionid_from_visit_name(
                            ispyb_info["ispyb_visit"]
                        )
                        matches = self.execute(
                            "select datacollectionid, imagedirectory, filetemplate from DataCollection "
                            "where sessionid=%s;",
                            sessionid,
                        )
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

    def get_sample_dcids(self, ispyb_info):
        dcid = ispyb_info.get("ispyb_dcid")
        sample_id = ispyb_info["ispyb_dc_info"].get("BLSAMPLEID")
        if not dcid or not sample_id:
            return None

        _enable_future()
        try:
            sample = _ispyb_api().get_sample(sample_id)
        except mysql.connector.errors.ProgrammingError as e:
            logger.debug(
                f"Error looking up sample for dcid={dcid}:\n{e}",
                exc_info=True,
            )
        except AttributeError as e:
            logger.debug(
                f"sample not yet supported by ispyb-api version:\n{e}",
                exc_info=True,
            )
        else:
            if sample:
                related_dcids = {
                    "dcids": sample.dcids,
                    "sample_id": sample.id,
                    "name": sample.name,
                }

            logger.debug(f"dcids defined via BLSample for dcid={dcid}: {related_dcids}")
            return related_dcids

    def get_related_dcids_same_directory(self, ispyb_info):
        dcid = ispyb_info.get("ispyb_dcid")
        if not dcid:
            return None

        sql_str = f"""
SELECT dc2.dataCollectionId
FROM DataCollection AS dc1
INNER JOIN DataCollection AS dc2
ON dc1.imageDirectory = dc2.imageDirectory and dc1.dataCollectionId <> dc2.dataCollectionId and dc1.imageDirectory is not NULL
WHERE dc1.dataCollectionId='{dcid}';"""
        return {"dcids": [row[0] for row in self.execute(sql_str)]}

    def get_space_group_and_unit_cell(self, dc_id):
        spacegroups = self.execute(
            "SELECT c.spaceGroup, c.cell_a, c.cell_b, c.cell_c, "
            " c.cell_alpha, c.cell_beta, c.cell_gamma "
            "FROM Crystal c "
            "JOIN BLSample b ON (b.crystalId = c.crystalId) "
            "JOIN DataCollection d ON (d.BLSAMPLEID = b.blSampleId) "
            "WHERE d.DataCollectionID = %s "
            "LIMIT 1;",
            dc_id,
        )
        if not spacegroups:
            return "", False
        cell = tuple(spacegroups[0][1:7])
        if not all(cell):
            cell = False
        return spacegroups[0][0], cell

    def get_energy_scan_from_dcid(self, dc_id):
        def __energy_offset(row):
            energy = 12398.42 / row["wavelength"]
            pk_energy = row["peakenergy"]
            if_energy = row["inflectionenergy"]

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

        s = """SELECT
    EnergyScan.energyscanid,
    EnergyScan.element,
    EnergyScan.peakenergy,
    EnergyScan.peakfprime,
    EnergyScan.peakfdoubleprime,
    EnergyScan.inflectionenergy,
    EnergyScan.inflectionfprime,
    EnergyScan.inflectionfdoubleprime,
    DataCollection.wavelength,
    BLSample.blsampleid as dcidsampleid,
    BLSampleProtein.blsampleid as protsampleid
FROM
    DataCollection
        INNER JOIN
    BLSample ON BLSample.blsampleid = DataCollection.blsampleid
        INNER JOIN
    Crystal ON Crystal.crystalid = BLSample.crystalid
        INNER JOIN
    Protein ON Protein.proteinid = Crystal.proteinid
        INNER JOIN
    Crystal CrystalProtein ON Protein.proteinid = CrystalProtein.proteinid
        INNER JOIN
    BLSample BLSampleProtein ON CrystalProtein.crystalid = BLSampleProtein.crystalid
        INNER JOIN
    EnergyScan ON DataCollection.sessionid = EnergyScan.sessionid
        AND BLSampleProtein.blsampleid = EnergyScan.blsampleid
WHERE
    DataCollection.datacollectionid = %s
        AND EnergyScan.element IS NOT NULL
"""
        labels = (
            "energyscanid",
            "element",
            "peakenergy",
            "peakfprime",
            "peakfdoubleprime",
            "inflectionenergy",
            "inflectionfprime",
            "inflectionfdoubleprime",
            "wavelength",
            "dcidsampleid",
            "protsampleid",
        )
        try:
            all_rows = [dict(zip(labels, r)) for r in self.execute(s, dc_id)]
            rows = [r for r in all_rows if r["dcidsampleid"] == r["protsampleid"]]
            if not rows:
                rows = all_rows
            energy_scan = min(rows, key=__energy_offset)
            edge_position = __select_edge_position(
                energy_scan["wavelength"],
                energy_scan["peakenergy"],
                energy_scan["inflectionenergy"],
            )
            res = {
                "energyscanid": energy_scan["energyscanid"],
                "atom_type": energy_scan["element"],
                "edge_position": edge_position,
            }
            if edge_position == "peak":
                res.update(
                    {
                        "fp": energy_scan["peakfprime"],
                        "fpp": energy_scan["peakfdoubleprime"],
                    }
                )
            else:
                if edge_position == "infl":
                    res.update(
                        {
                            "fp": energy_scan["inflectionfprime"],
                            "fpp": energy_scan["inflectionfdoubleprime"],
                        }
                    )
        except Exception:
            self.log.debug("Matching energy scan data for dcid %s not available", dc_id)
            res = {}
        return res

    def get_protein_from_dcid(self, dc_id):

        s = """SELECT
    Protein.proteinid,
    Protein.name,
    Protein.acronym,
    Protein.proteintype,
    Protein.sequence
FROM
    Protein
        INNER JOIN
    Crystal ON Crystal.proteinid = Protein.proteinid
        INNER JOIN
    BLSample ON BLSample.crystalid = Crystal.crystalid
        INNER JOIN
    DataCollection ON DataCollection.blsampleid = BLSample.blsampleid
WHERE
    DataCollection.datacollectionid = %s
"""
        results = self.execute(s, dc_id)
        labels = ("proteinid", "name", "acronym", "proteintype", "sequence")
        try:
            assert len(results) == 1, len(results)
            assert len(results[0]) == len(labels), results[0]
            res = dict(zip(labels, results[0]))
            return res
        except Exception:
            self.log.debug("Cannot find protein information for dcid %s", dc_id)

    def get_dcid_for_filename(self, filename):
        basename, extension = os.path.splitext(filename)
        if extension:
            extension = extension.lstrip(".")
        if basename.endswith("_master"):
            basename = basename[:-7]
        m = re.match(r"(.*)_#+$", basename)
        if m:
            basename = m.group(1)
        m = re.match(r"(.*)_([0-9]+)$", basename)
        if m:
            dcn = int(m.group(2))
            basename = m.group(1)
        else:
            dcn = None
        if not basename:
            raise ValueError("Could not determine prefix of %r" % filename)

        if dcn is None:
            results = self.execute(
                "SELECT dataCollectionId, imageDirectory, imageSuffix, fileTemplate "
                "FROM DataCollection "
                "WHERE imagePrefix = %s "
                "LIMIT 101;",
                basename,
            )
        else:
            results = self.execute(
                "SELECT dataCollectionId, imageDirectory, imageSuffix, fileTemplate "
                "FROM DataCollection "
                "WHERE (imagePrefix = %s) AND (dataCollectionNumber = %s)"
                "LIMIT 101;",
                (basename, dcn),
            )
        if len(results) > 100:
            raise ValueError("Too many candidates found for %r" % filename)
        if extension:
            results = [r for r in results if r[2] == extension]
        if not results:
            raise ValueError("Could not find any candidates for %r" % filename)

        candidates = [r for r in results if r[3] == filename]
        if candidates:
            results = candidates

        if len(results) == 1:
            return results[0][0]

        raise ValueError(
            "Multiple matching candidates found:\n"
            + "\n".join("DCID %d %s%s" % (r[0], r[1], r[3]) for r in results)
        )

    def get_dcid_for_path(self, path):
        """Take a file path and try to identify a best match DCID"""
        if "/" not in path:
            return self.get_dcid_for_filename(path)
        if not path.startswith("/"):
            raise ValueError("Need absolute file path instead of %r" % path)
        extension = os.path.splitext(path)[1].lstrip(".")
        basepath, filename = os.path.split(path)
        basepath = basepath + "/"
        if extension:
            altpath = "__no_alternative__"
        else:
            altpath = path.rstrip("/") + "/"
        results = self.execute(
            "SELECT dataCollectionId, imageDirectory, imagePrefix, imageSuffix, fileTemplate "
            "FROM DataCollection "
            "WHERE imageDirectory = %s OR imageDirectory = %s;",
            (basepath, altpath),
        )
        if extension:
            results = [r for r in results if r[3] == extension]
        if not results:
            raise ValueError("No matching DCID identified for %r" % path)

        if filename:
            candidates = [r for r in results if r[4].startswith(filename)]
            if candidates:
                results = candidates
            candidates = [r for r in results if r[4] == filename]
            if candidates:
                results = candidates
            candidates = [r for r in results if filename.startswith(r[2])]
            if candidates:
                results = candidates
                prefix_lengths = [
                    len(os.path.commonprefix((r[4], filename))) for r in results
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
        number_of_images = dc_info.get("numberOfImages")
        axis_range = dc_info.get("axisRange")
        if number_of_images is None or axis_range is None:
            return None
        return number_of_images > 1 and axis_range == 0.0

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

    def get_bl_sessionid_from_visit_name(self, visit_name):
        m = re.match(r"([a-z][a-z])([\d]+)[-]([\d]+)", visit_name)
        assert m is not None
        assert len(m.groups()) == 3
        proposal_code, proposal_number, visit_number = m.groups()
        sql_str = """
SELECT sessionId
FROM BLSession bs
INNER JOIN Proposal p
ON bs.proposalId = p.proposalId
WHERE p.proposalcode='%s' and p.proposalnumber='%s' and bs.visit_number='%s'
;""" % (
            proposal_code,
            proposal_number,
            visit_number,
        )
        results = self.execute(sql_str)
        assert len(results) == 1
        return results[0][0]

    def get_diffractionplan_from_dcid(self, dc_id):
        sql_str = """
SELECT
    dp.diffractionplanid,
    dp.experimentkind,
    dp.centringmethod,
    dp.preferredbeamsizex,
    dp.preferredbeamsizey,
    dp.exposuretime,
    dp.requiredresolution,
    dp.radiationsensitivity,
    dp.anomalousscatterer,
    dp.energy
FROM
    DiffractionPlan AS dp
        INNER JOIN
    BLSample AS bls ON bls.diffractionplanid = dp.diffractionplanid
        INNER JOIN
    DataCollection AS dc ON dc.blsampleid = bls.blsampleid
WHERE
    dc.datacollectionid='%s'
;""" % str(
            dc_id
        )
        results = self.execute(sql_str)

        labels = (
            "diffractionplanid",
            "experimentkind",
            "centringmethod",
            "preferredbeamsizex",
            "preferredbeamsizey",
            "exposuretime",
            "requiredresolution",
            "radiationsensitivity",
            "anomalousscatterer",
            "energy",
        )
        try:
            assert len(results) == 1, len(results)
            assert len(results[0]) == len(labels), results[0]
            res = dict(zip(labels, results[0]))
            return res
        except Exception:
            self.log.debug(
                "Cannot find diffraction plan information for dcid %s", dc_id
            )


def ready_for_processing(message, parameters):
    """Check whether this message is ready for templatization."""
    if not parameters.get("ispyb_wait_for_runstatus"):
        return True

    if not parameters.get("ispyb_dcid"):
        return True

    dc = _ispyb_api().get_data_collection(parameters["ispyb_dcid"])
    return dc.status is not None


def ispyb_filter(message, parameters):
    """Do something to work out what to do with this data..."""

    i = ispybtbx()

    message, parameters = i(message, parameters)

    processingjob_id = parameters.get(
        "ispyb_reprocessing_id", parameters.get("ispyb_process")
    )
    if processingjob_id:
        parameters["ispyb_processing_job"] = _ispyb_api().get_processing_job(
            processingjob_id
        )
        if "ispyb_dcid" not in parameters:
            parameters["ispyb_dcid"] = parameters["ispyb_processing_job"].DCID

    if "ispyb_dcid" not in parameters:
        return message, parameters

    # FIXME put in here logic to check input if set i.e. if dc_id==0 then check
    # files exist; if image already set check they exist, ...

    dc_id = parameters["ispyb_dcid"]

    dc_info = i.get_dc_info(dc_id)
    dc_info["uuid"] = parameters.get("guid") or str(uuid.uuid4())
    parameters["ispyb_beamline"] = i.get_beamline_from_dcid(dc_id)
    if str(parameters["ispyb_beamline"]).lower() in _gpfs03_beamlines:
        parameters["ispyb_preferred_datacentre"] = "hamilton"
    else:
        parameters["ispyb_preferred_datacentre"] = "cluster"
    parameters["ispyb_detectorclass"] = i.dc_info_to_detectorclass(dc_info)
    parameters["ispyb_dc_info"] = dc_info
    dc_class = i.classify_dc(dc_info)
    parameters["ispyb_dc_class"] = dc_class
    diff_plan_info = i.get_diffractionplan_from_dcid(dc_id)
    parameters["ispyb_diffraction_plan"] = diff_plan_info
    protein_info = i.get_protein_from_dcid(dc_id)
    parameters["ispyb_protein_info"] = protein_info
    energy_scan_info = i.get_energy_scan_from_dcid(dc_id)
    parameters["ispyb_energy_scan_info"] = energy_scan_info
    start, end = i.dc_info_to_start_end(dc_info)
    if dc_class["grid"] and dc_info["dataCollectionGroupId"]:
        try:
            gridinfo = i.get_gridscan_info(dc_info["dataCollectionGroupId"])
            if gridinfo:
                # FIXME: timestamps can not be JSON-serialized
                if "recordTimeStamp" in gridinfo:
                    del gridinfo["recordTimeStamp"]
                parameters["ispyb_dc_info"]["gridinfo"] = gridinfo
        except ispyb.NoResult:
            pass
    parameters["ispyb_preferred_processing"] = "xia2/DIALS"
    if dc_info.get("dataCollectionGroupId"):
        try:
            container = (
                _ispyb_api()
                .get_data_collection_group(dc_info["dataCollectionGroupId"])
                .container
            )
            if container:
                parameters["ispyb_preferred_processing"] = container.priority_processing
        except ispyb.NoResult:
            pass
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

    space_group, cell = i.get_space_group_and_unit_cell(dc_id)
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
    parameters["ispyb_related_dcids"] = i.get_sample_group_dcids(parameters)
    if parameters["ispyb_dc_info"].get("BLSAMPLEID"):
        # if a sample is linked to the dc, then get dcids on the same sample
        related_dcids = i.get_sample_dcids(parameters)
    else:
        # else get dcids collected into the same image directory
        related_dcids = i.get_related_dcids_same_directory(parameters)
    if related_dcids:
        parameters["ispyb_related_dcids"].append(related_dcids)
    logger.debug(f"ispyb_related_dcids: {parameters['ispyb_related_dcids']}")

    if (
        "ispyb_processing_job" in parameters
        and parameters["ispyb_processing_job"].recipe
        and not message.get("recipes")
        and not message.get("custom_recipe")
    ):
        # Prefix recipe name coming from ispyb/synchweb with 'ispyb-'
        message["recipes"] = ["ispyb-" + parameters["ispyb_processing_job"].recipe]
        return message, parameters

    if dc_class["grid"]:
        if parameters["ispyb_beamline"] == "i02-2":
            message["default_recipe"] = ["archive-nexus", "vmxi-spot-counts-per-image"]
        else:
            message["default_recipe"] = ["per-image-analysis-gridscan"]
        return message, parameters

    if dc_class["screen"]:
        message["default_recipe"] = [
            "per-image-analysis-rotation",
            "strategy-edna",
            "strategy-mosflm",
        ]
        parameters["ispyb_images"] = ""
        return message, parameters

    if not dc_class["rotation"]:
        # possibly EM dataset
        message["default_recipe"] = []
        return message, parameters

    # for the moment we do not want multi-xia2 for /dls/mx i.e. VMXi
    # beware if other projects start using this directory structure will
    # need to be smarter here...

    if dc_info["dataCollectionGroupId"]:
        related_dcs = i.get_related_dcs(dc_info["dataCollectionGroupId"])
        if parameters["ispyb_image_directory"].startswith("/dls/mx"):
            related = []
        else:
            related = list(sorted(set(related_dcs)))
        for dc in related_dcs:
            info = i.get_dc_info(dc)
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

            info = i.get_dc_info(dc)
            other_dc_class = i.classify_dc(info)
            if other_dc_class["rotation"]:
                start, end = i.dc_info_to_start_end(info)

                related_images.append(
                    "%s:%d:%d" % (i.dc_info_to_filename(info), start, end)
                )

            parameters["ispyb_images"] = ",".join(related_images)

    message["default_recipe"] = [
        "per-image-analysis-rotation",
        "processing-autoproc",
        "processing-fast-dp",
        "processing-rlv",
        "processing-xia2-3dii",
        "processing-xia2-dials",
    ]

    if parameters["ispyb_beamline"] == "i02-2":
        message["default_recipe"] = [
            "archive-nexus",
            "processing-autoproc",
            "processing-fast-dp",
            "processing-xia2-3dii",
            "processing-xia2-dials",
            "vmxi-per-image-analysis",
        ]

    if parameters["ispyb_images"]:
        message["default_recipe"].append("processing-multi-xia2-dials")
        message["default_recipe"].append("processing-multi-xia2-3dii")

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

    for dc_id in dc_ids:
        message = {}
        parameters = {"ispyb_dcid": dc_id}
        message, parameters = ispyb_filter(message, parameters)

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
