from __future__ import annotations

import os
import pathlib
import re
from datetime import datetime, timedelta
from time import time
from typing import Any, Dict, List, Literal, Mapping, Optional

import gemmi
import ispyb
import prometheus_client
import pydantic
import sqlalchemy.engine
import sqlalchemy.orm
import workflows.recipe
from ispyb.sqlalchemy import (
    AutoProc,
    AutoProcIntegration,
    AutoProcProgram,
    AutoProcProgramAttachment,
    AutoProcScaling,
    AutoProcScalingHasInt,
    BLSession,
    DataCollection,
    ProcessingJob,
    Proposal,
    Protein,
)
from sqlalchemy import or_
from sqlalchemy.orm import Load, contains_eager, joinedload
from workflows.recipe.wrapper import RecipeWrapper
from workflows.services.common_service import CommonService

import dlstbx.ispybtbx
from dlstbx.crud import get_protein_for_dcid
from dlstbx.util import INDUSTRIAL_CODES, ChainMapWithReplacement
from dlstbx.util.metal_id_helpers import dcids_from_related_dcids
from dlstbx.util.pdb import PDBFileOrCode, trim_pdb_bfactors
from dlstbx.util.prometheus_metrics import BasePrometheusMetrics, NoMetrics


class PrometheusMetrics(BasePrometheusMetrics):
    def create_metrics(self):
        self.zocalo_trigger_jobs_total = prometheus_client.Counter(
            name="zocalo_trigger_jobs_total",
            documentation="The total number of jobs triggered by the Zocalo trigger service",
            labelnames=["target"],
        )


class DimpleParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    experiment_type: str
    scaling_id: int = pydantic.Field(gt=0)
    mtz: pathlib.Path | Dict[str, pathlib.Path]
    pdb: list[PDBFileOrCode]
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    symlink: str = pydantic.Field(default="")


class DataCollectionInfo(pydantic.BaseModel):
    numberOfImages: int
    startImageNumber: int
    imagePrefix: Optional[str] = None
    dataCollectionNumber: Optional[int] = None
    SESSIONID: Optional[int] = None
    wavelength: float = pydantic.Field(gt=0)


class RelatedDCIDs(pydantic.BaseModel):
    dcids: List[int]
    sample_id: Optional[int] = pydantic.Field(gt=0, default=None)
    sample_group_id: Optional[int] = pydantic.Field(gt=0, default=None)


class MetalIdParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    dcg_dcids: list[int]
    related_dcids: list[RelatedDCIDs]
    dc_info: DataCollectionInfo
    proc_prog: str
    experiment_type: str
    beamline: str
    scaling_id: int = pydantic.Field(gt=0)
    autoprocprogram_id: int = pydantic.Field(gt=0)
    pdb: list[PDBFileOrCode]
    energy_min_diff: float = pydantic.Field(default=10, gt=0)
    timeout: float = pydantic.Field(default=360, alias="timeout-minutes")
    backoff_delay: float = pydantic.Field(default=5, alias="backoff-delay")
    backoff_multiplier: float = pydantic.Field(default=2, alias="backoff-multiplier")
    automatic: Optional[bool] = False
    comment: Optional[str] = None


class ProteinInfo(pydantic.BaseModel):
    sequence: Optional[str] = None


class MrBumpParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    experiment_type: str
    scaling_id: int = pydantic.Field(gt=0)
    protein_info: Optional[ProteinInfo] = None
    hklin: pathlib.Path | Dict[str, pathlib.Path]
    pdb: list[PDBFileOrCode]
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    recipe: Optional[str] = None


class DiffractionPlanInfo(pydantic.BaseModel):
    anomalousScatterer: Optional[str] = None


class EPPredictParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    diffraction_plan_info: Optional[DiffractionPlanInfo] = None
    program: str
    program_id: int = pydantic.Field(gt=0)
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    data: pathlib.Path
    threshold: float


class MRPredictParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    diffraction_plan_info: Optional[DiffractionPlanInfo] = None
    program: str
    program_id: int = pydantic.Field(gt=0)
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    data: pathlib.Path
    threshold: float


class Screen19MXParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    visit: str
    test_visit: str
    program_id: int = pydantic.Field(gt=0)
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    data: pathlib.Path


class BigEPParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    experiment_type: str
    diffraction_plan_info: Optional[DiffractionPlanInfo] = None
    scaling_id: int = pydantic.Field(gt=0)
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    spacegroup: Optional[str] = None
    recipe: Optional[str] = None
    upstream_source: Optional[str] = None
    data: Optional[str] = None
    scaled_unmerged_mtz: Optional[str] = None

    @pydantic.field_validator("spacegroup")
    def is_spacegroup_null(cls, v):
        ## Validate space group parameter and exclude "None"
        try:
            spg = gemmi.SpaceGroup(v).short_name()
        except (TypeError, ValueError):
            # Note: Is this valid? Don't we want to raise ValueError?
            return None
        return spg


class BigEPLauncherParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    pipeline: Literal["autoSHARP", "AutoBuild", "Crank2"]
    data: pathlib.Path
    shelxc_path: pathlib.Path
    fast_ep_path: pathlib.Path
    program_id: int = pydantic.Field(gt=0)
    scaling_id: int = pydantic.Field(gt=0)
    path_ext: Optional[str] = pydantic.Field(
        default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    recipe: Optional[str] = None
    upstream_source: Optional[str] = None


class FastEPParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    experiment_type: str
    diffraction_plan_info: Optional[DiffractionPlanInfo] = None
    scaling_id: int = pydantic.Field(gt=0)
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    mtz: pathlib.Path | Dict[str, pathlib.Path]
    recipe: Optional[str] = None


class BestParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    program_id: int = pydantic.Field(gt=0)
    data: pathlib.Path
    automatic: Optional[bool] = False
    comment: Optional[str] = None


class MultiplexParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    related_dcids: List[RelatedDCIDs]
    program_id: Optional[int] = pydantic.Field(default=0, gt=0)
    wavelength: Optional[float] = pydantic.Field(default=None, gt=0)
    spacegroup: Optional[str] = None
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    backoff_delay: float = pydantic.Field(default=8, alias="backoff-delay")
    backoff_max_try: int = pydantic.Field(default=10, alias="backoff-max-try")
    backoff_multiplier: float = pydantic.Field(default=2, alias="backoff-multiplier")
    wavelength_tolerance: float = pydantic.Field(default=1e-4, ge=0)
    diffraction_plan_info: Optional[DiffractionPlanInfo] = None
    recipe: Optional[str] = None
    use_clustering: Optional[List[str]] = None


class Xia2SsxReduceParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    related_dcids: List[RelatedDCIDs]
    wavelength: Optional[float] = pydantic.Field(default=None, gt=0)
    spacegroup: Optional[str] = None
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    backoff_delay: float = pydantic.Field(default=8, alias="backoff-delay")
    backoff_max_try: int = pydantic.Field(default=10, alias="backoff-max-try")
    backoff_multiplier: float = pydantic.Field(default=2, alias="backoff-multiplier")
    wavelength_tolerance: float = pydantic.Field(default=1e-4, ge=0)


class AlphaFoldParameters(pydantic.BaseModel):
    protein_id: int = pydantic.Field(gt=0)


class ShelxtParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    ins_file_location: pathlib.Path
    prefix: Optional[str] = None
    automatic: Optional[bool] = False
    scaling_id: int = pydantic.Field(gt=0)
    comment: Optional[str] = None


class LigandFitParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    pdb: pathlib.Path
    mtz: pathlib.Path
    smiles: str
    pipeline: str
    beamline: str
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    scaling_id: list[int]
    min_cc_keep: float = pydantic.Field(default=0.7)


class AlignCrystalParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    comment: Optional[str] = None
    upstream_pipeline: Literal["xia2-dials", "fast_dp"]
    experiment_type: str
    experiment_file: pathlib.Path
    symlink: str = pydantic.Field(default="")


class DLSTrigger(CommonService):
    """A service that creates and runs downstream processing jobs."""

    # Human readable service name
    _service_name = "DLS Trigger"

    # Logger name
    _logger_name = "dlstbx.services.trigger"

    def initializing(self):
        """Subscribe to the trigger queue. Received messages must be acknowledged."""
        self._ispyb_sessionmaker = sqlalchemy.orm.sessionmaker(
            bind=sqlalchemy.create_engine(
                ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
            )
        )
        workflows.recipe.wrap_subscribe(
            self._transport,
            "trigger",
            self.trigger,
            acknowledgement=True,
            log_extender=self.extend_log,
        )
        self.ispyb = ispyb.open()

        # Initialise metrics if requested
        if self._environment.get("metrics"):
            self._metrics = PrometheusMetrics()
        else:
            self._metrics = NoMetrics()

    def trigger(self, rw, header, message):
        """Forward the trigger message to a specific trigger function."""
        # Extract trigger target from the recipe
        params = rw.recipe_step.get("parameters", {})
        target = params.get("target")
        if not target:
            self.log.error("No trigger target defined in recipe")
            rw.transport.nack(header)
            return
        if not hasattr(self, "trigger_" + target):
            self.log.error("Unknown target %s defined in recipe", target)
            rw.transport.nack(header)
            return

        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.set_default_channel("output")

        parameter_map = ChainMapWithReplacement(
            rw.recipe_step["parameters"].get("ispyb_parameters") or {},
            message if isinstance(message, dict) else {},
            rw.recipe_step["parameters"],
            substitutions=rw.environment,
        )

        with self._ispyb_sessionmaker() as session:
            try:
                result = getattr(self, "trigger_" + target)(
                    rw=rw,
                    header=header,
                    message=message or {},
                    parameters=parameter_map,
                    parameter_map=parameter_map,
                    session=session,
                    transaction=txn,
                )
            except pydantic.ValidationError as e:
                self.log.error(
                    f"{target.capitalize()} trigger called with invalid parameters: {e}"
                )
                result = None

        if result and result.get("success"):
            rw.send({"result": result.get("return_value")}, transaction=txn)
            rw.transport.ack(header, transaction=txn)
            if retval := result.get("return_value"):
                if isinstance(retval, (tuple, list)):
                    for i in range(len(retval)):
                        self._metrics.record_metric(
                            "zocalo_trigger_jobs_total", [target]
                        )
                else:
                    self._metrics.record_metric("zocalo_trigger_jobs_total", [target])
        else:
            rw.transport.transaction_abort(txn)
            rw.transport.nack(header)
            return
        rw.transport.transaction_commit(txn)

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_dimple(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        parameters: DimpleParameters,
        session: sqlalchemy.orm.session.Session,
        **kwargs,
    ):
        """Trigger a dimple job for a given data collection.

        Identify any PDB files or PDB codes associated with the given data collection.
        - PDB codes or file contents stored in the ISPyB PDB table and linked with
          the given data collection. Any files defined in the database will be copied
          into a subdirectory inside `pdb_tmpdir`, where the subdirectory name will be
          a hash of the file contents.
        - PDB files (with `.pdb` extension) stored in the directory optionally provided
          by the `user_pdb_directory` recipe parameter.

        If any PDB files or codes are identified, then new ProcessingJob,
        ProcessingJobImageSweep and ProcessingJobParameter will be created, and the
        resulting processingJobId will be sent to the `processing_recipe` queue.

        Recipe parameters:
        - target: set this to "dimple"
        - dcid: the dataCollectionId for the given data collection
        - comment: a comment to be stored in the ProcessingJob.comment field
        - automatic: boolean value passed to ProcessingJob.automatic field
        - scaling_id: autoProcScalingId that the dimple results should be linked to
        - mtz: the input mtz reflection file for dimple
        - user_pdb_directory: optionally look for PDB files in this directory
        - pdb_tmpdir: temporary location to write the contents of PDB files stored
            in the database

        Minimal recipe parameters:
        {
            "target": "dimple",
            "dcid": 123456,
            "comment": "DIMPLE triggered by automatic xia2-dials",
            "automatic": True,
            "scaling_id": 654321,
            "user_pdb_directory": "/path/to/user_pdb",
            "mtz": "/path/to/scaled.mtz",
            "pdb_tmpdir": "/path/to/pdb_tmpdir",
        }
        """

        if parameters.experiment_type not in (
            "OSC",
            "SAD",
            "MAD",
            "Helical",
            "Metal ID",
        ):
            self.log.info(
                f"Skipping dimple trigger: experiment type {parameters.experiment_type} not supported"
            )
            return {"success": True}

        dcid = parameters.dcid

        pdb_files_or_codes = parameters.pdb

        if not pdb_files_or_codes:
            self.log.info(
                "Skipping dimple trigger: DCID %s has no associated PDB information",
                dcid,
            )
            return {"success": True}
        pdb_files = [str(p) for p in pdb_files_or_codes]
        self.log.info("PDB files: %s", ", ".join(pdb_files))

        dc = (
            session.query(DataCollection)
            .filter(DataCollection.dataCollectionId == dcid)
            .one()
        )
        if isinstance(parameters.mtz, dict):
            query = (
                session.query(
                    AutoProcScaling.autoProcScalingId,
                    AutoProcProgram.processingPrograms,
                    AutoProcProgramAttachment.filePath,
                    AutoProcProgramAttachment.fileName,
                )
                .join(
                    AutoProcScalingHasInt,
                    AutoProcScalingHasInt.autoProcScalingId
                    == AutoProcScaling.autoProcScalingId,
                )
                .join(
                    AutoProcIntegration,
                    AutoProcIntegration.autoProcIntegrationId
                    == AutoProcScalingHasInt.autoProcIntegrationId,
                )
                .join(
                    AutoProcProgram,
                    AutoProcProgram.autoProcProgramId
                    == AutoProcIntegration.autoProcProgramId,
                )
                .join(
                    AutoProcProgramAttachment,
                    AutoProcProgramAttachment.autoProcProgramId
                    == AutoProcProgram.autoProcProgramId,
                )
                .filter(AutoProcScaling.autoProcScalingId == parameters.scaling_id)
            )
            attachments = query.all()
            for _, program_name, filepath, filename in attachments:
                if filename == str(parameters.mtz.get(program_name)):
                    datafile = pathlib.Path(filepath) / filename
                    break
            else:
                self.log.error(
                    "Skipping dimple trigger: No input data files found for ScalingId %s",
                    parameters.scaling_id,
                )
                return {"success": True}
        elif isinstance(parameters.mtz, pathlib.Path):
            datafile = parameters.mtz.as_posix()
        else:
            self.log.error(
                "Skipping dimple trigger: Invalid input data type path %s",
                type(parameters.mtz),
            )
            return {"success": True}

        dimple_parameters: dict[str, list[Any]] = {
            "data": [os.fspath(datafile)],
            "scaling_id": [parameters.scaling_id],
            "pdb": pdb_files,
            "create_symlink": [parameters.symlink],
        }

        jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
        jisp["datacollectionid"] = dcid
        jisp["start_image"] = dc.startImageNumber
        jisp["end_image"] = dc.startImageNumber + dc.numberOfImages - 1

        self.log.debug("Dimple trigger: Starting")

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = parameters.automatic
        jp["comments"] = parameters.comment
        jp["datacollectionid"] = dcid
        jp["display_name"] = "DIMPLE"
        jp["recipe"] = "postprocessing-dimple"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"Dimple trigger: generated JobID {jobid}")

        for key, values in dimple_parameters.items():
            for value in values:
                jpp = self.ispyb.mx_processing.get_job_parameter_params()
                jpp["job_id"] = jobid
                jpp["parameter_key"] = key
                jpp["parameter_value"] = value
                jppid = self.ispyb.mx_processing.upsert_job_parameter(
                    list(jpp.values())
                )
                self.log.debug(f"Dimple trigger: generated JobParameterID {jppid}")

        jisp["job_id"] = jobid
        jispid = self.ispyb.mx_processing.upsert_job_image_sweep(list(jisp.values()))
        self.log.debug(f"Dimple trigger: generated JobImageSweepID {jispid}")

        self.log.debug(f"Dimple trigger: Processing job {jobid} created")

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info(f"Dimple trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_metal_id(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        message: Dict,
        parameters: MetalIdParameters,
        session: sqlalchemy.orm.session.Session,
        transaction: int,
        **kwargs,
    ):
        """Trigger a metal_id job for a given data collection.

        Requires experiment type to be "Metal ID" and for data collections to be in
        the same data collection group, or for data collections to be recorded for
        the same sample in the same visit and for the image prefix to contain E# where
        # is an integer. Metal ID will trigger for every other data collection in the
        group, with the assumption that data collections alternate above and below
        metal absorption edges.

        Trigger also requires a PDB file or code to be associated with the given data
        collection:
        - PDB codes or file contents stored in the ISPyB PDB table and linked with
          the given data collection. Any files defined in the database will be copied
          into a subdirectory inside `pdb_tmpdir`, where the subdirectory name will be
          a hash of the file contents.
        - PDB files (with `.pdb` extension) stored in the directory optionally provided
          by the `user_pdb_directory` recipe parameter.

        If any PDB files or codes are identified, then new ProcessingJob,
        ProcessingJobImageSweep and ProcessingJobParameter will be created, and the
        resulting processingJobId will be sent to the `processing_recipe` queue.

        Recipe parameters are described below with appropriate ispyb placeholder "{}"
        values:
        - target: set this to "metal_id"
        - dcid: the dataCollectionId for the given data collection i.e. "{ispyb_dcid}"
        - dcids: the dataCollectionIDs preceding the current dcid in the data
        collection group. i.e. "{$REPLACE:ispyb_dcg_dcids}"
        - proc_prog: The name, as it appears in ISPyB, of the autoprocessing pipeline
        for which the output will be used as the metal_id input mtz file.
        - experiment_type: the experiment type of the data collection (i.e.
         "{ispyb_dcg_experiment_type}").
        - beamline: The beamline that the data collection was recorded on (i.e.
        "{ispyb_beamline}").
        - comment: a comment to be stored in the ProcessingJob.comment field
        - automatic: boolean value passed to ProcessingJob.automatic field
        - scaling_id: autoProcScalingId that the metal_id results should be linked to
        - pdb: list of pdb files or codes provided in the pdb_files_or_codes_format,
        where each pdb file or code is provided as a dict with keys of "filepath",
        "code" and "source". Set the filepath or code and set the other values to null.
        "{$REPLACE:ispyb_pdb}" will also achieve this.
        - energy_min_diff - (optional) the minimum energy difference (eV) required between
        data collections taken above and below the metal absorption edge.
        - timeout-minutes - (optional) the max time (in minutes) allowed wait for
        processing jobs to finish before skipping
        - backoff-delay - (optional) the time (in minutes) that message will be delayed by when
        checkpointing if waiting for processing to finish
        - backoff-multiplier - (optional) a multipler by which the delay is increased after
        successive checkpoints.

        Example recipe parameters:
        { "target": "metal_id",
            "dcid": 123456,
            "dcg_dcids": [123453, 123454, 123455],
            "experiment_type": "Metal ID",
            "related_dcids":[{'dcids': [123453, 123454, 123455], 'sample_id': 54321, 'name': 'test_sample'}],
            "beamline": "i03"
            "proc_prog": "xia2 dials",
            "comment": "Metal_ID triggered by xia2 dials",
            "automatic": true,
            "scaling_id": 654321,
            "pdb": [
                {
                    "filepath": "/path/to/file.pdb",
                    "code": null,
                    "source": null
            }],
            "energy_min_diff": 10.0
            "timeout-minutes": 60,
            "backoff-delay": 4,
            "backoff-multiplier": 2
        }
        """
        if parameters.experiment_type not in (
            "Metal ID",
            "OSC",
            "SAD",
            "MAD",
            "Helical",
        ):
            self.log.info(
                f"Skipping metal id trigger: experiment type {parameters.experiment_type} not supported"
            )
            return {"success": True}

        if parameters.experiment_type != "Metal ID" and parameters.beamline != "i23":
            self.log.info(
                f"Skipping metal id trigger: experiment type {parameters.experiment_type} not supported for beamline {parameters.beamline}"
            )
            return {"success": True}

        pdb_files_or_codes = parameters.pdb

        if not pdb_files_or_codes:
            self.log.info(
                f"Skipping metal id trigger: DCID {parameters.dcid} has no associated PDB information"
            )
            return {"success": True}

        pdb_files = [str(p) for p in pdb_files_or_codes]
        self.log.info("PDB files: %s", ", ".join(pdb_files))

        proc_prog = parameters.proc_prog

        # Dict of file patterns for each of the autoprocessing pipelines
        input_file_patterns = {
            "fast_dp": "fast_dp.mtz",
            "xia2 dials": "_free.mtz",
            "xia2 3dii": "_free.mtz",
            "xia2.multiplex": "scaled.mtz",
            "autoPROC": "truncate-unique.mtz",
            "autoPROC+STARANISO": "staraniso_alldata-unique.mtz",
        }

        if proc_prog not in input_file_patterns:
            self.log.info(
                f"Skipping metal id trigger: {proc_prog} is not an accepted upstream processing pipeline for metal id"
            )
            return {"success": True}

        # Look for dcids in checkpointed message
        dcids = message.get("dcids")
        if not dcids:
            self.log.info(
                f"Metal ID trigger: looking for matching data collection for dcid {parameters.dcid}"
            )

            if parameters.beamline == "i23":
                dcids = dcids_from_related_dcids(self.log, parameters, session)
                if not dcids:
                    return {"success": True}
            else:
                # Get a list of collections in the data collection group that include and are older than the present dcid
                dcids = [
                    dcid_value
                    for dcid_value in parameters.dcg_dcids
                    if dcid_value < parameters.dcid
                ]
                # If dcid is not included in the list of dcg_dcids it needs to be added
                if parameters.dcid not in dcids:
                    dcids.append(parameters.dcid)

                # Only want to trigger metal ID when an above/below pair is present (i.e. every other data collection)
                if len(dcids) % 2:
                    self.log.info(
                        "Skipping metal id trigger: Need even number of dcids"
                    )
                    return {"success": True}

                # Get just the most recent two dcids
                dcids = sorted(dcids)[-2::]

            self.log.info(f"Metal ID trigger: found dcids {dcids}")

        # On first time processing the message check that the photon energy is different enough between the two data collections
        ntry = message.get("ntry", 0)
        if not ntry:
            query = session.query(DataCollection).filter(
                DataCollection.dataCollectionId.in_(dcids)
            )
            wavelengths = []
            for dc in query.all():
                wavelengths.append(dc.wavelength)

            energy_diff = abs(12398.0 / wavelengths[0] - 12398.0 / wavelengths[1])
            if energy_diff < parameters.energy_min_diff:
                self.log.info(
                    f"Skipping metal id trigger - data collections {dcids} have energy difference < {parameters.energy_min_diff} eV"
                )
                return {"success": True}

        # Check that both dcids have finished processing successfully
        # Query the autoProcProgram table for the current job
        query = (
            session.query(
                AutoProcProgram.processingEnvironment,
                AutoProcProgramAttachment.filePath,
                AutoProcProgramAttachment.fileName,
            )
            .join(
                AutoProcProgramAttachment,
                AutoProcProgram.autoProcProgramId
                == AutoProcProgramAttachment.autoProcProgramId,
            )
            .filter(AutoProcProgram.autoProcProgramId == parameters.autoprocprogram_id)
            .filter(
                AutoProcProgramAttachment.fileName.endswith(
                    input_file_patterns[proc_prog]
                )
            )
            .one_or_none()
        )

        if not query:
            self.log.info(
                f"Skipping metal id trigger: No record found for autoProcProgramId {parameters.autoprocprogram_id}"
            )
            return {"success": True}

        processing_environment, file_path, file_name = query
        mtz_file_1 = pathlib.Path(file_path) / file_name
        self.log.info(f"Retrieved mtz file {mtz_file_1} from current data collection")

        # Find a matching data processing run for the other dcid. Must match proc_prog and processing_environment
        query = (
            (
                session.query(AutoProcProgram).join(
                    ProcessingJob,
                    ProcessingJob.processingJobId == AutoProcProgram.processingJobId,
                )
            )
            .filter(ProcessingJob.dataCollectionId == dcids[0])
            .filter(ProcessingJob.automatic == True)  # noqa E712
            .filter(AutoProcProgram.processingPrograms == proc_prog)
            .filter(AutoProcProgram.processingEnvironment == processing_environment)
        )
        if not len(query.all()):
            self.log.info(
                f"Skipping metal id trigger: No matching processing job found for autoProcProgramId {parameters.autoprocprogram_id} on dcid {dcids[0]}"
            )
            return {"success": True}
        # Look to see if any matching jobs have finished running - take the first one that has
        for matching_job in query.all():
            if matching_job.processingStatus == 1:
                self.log.info(
                    f"Metal ID trigger: Found matching job {matching_job.autoProcProgramId} - using as input to metal_id"
                )
                break
        else:
            self.log.info(
                f"Metal ID trigger: Waiting for processing to finish for autoProcProgramIds: {[job.autoProcProgramId for job in query.all()]}"
            )
            # Get previous checkpoint history
            start_time = message.get("start_time", time())
            run_time = time() - start_time
            if run_time > parameters.timeout * 60:
                self.log.warning(
                    f"Metal ID trigger: timeout exceeded waiting for matching job for dcid '{parameters.dcid}' and autoProcProgramId '{parameters.autoprocprogram_id}' to finish. Skipping trigger"
                )
                return {"success": True}
            # Calculate the message delay
            delay = parameters.backoff_delay * parameters.backoff_multiplier**ntry
            self.log.info(
                f"Metal ID trigger: Checkpointing for dcid '{parameters.dcid}' autoProcProgramId '{parameters.autoprocprogram_id}' with delay of {delay} minutes"
            )
            ntry += 1
            rw.checkpoint(
                {
                    "start_time": start_time,
                    "ntry": ntry,
                    "dcids": dcids,
                    "wavelengths": wavelengths,
                },
                delay=delay * 60,
                transaction=transaction,
            )
            return {"success": True}

        query = (
            session.query(AutoProcProgramAttachment)
            .filter(
                AutoProcProgramAttachment.autoProcProgramId
                == matching_job.autoProcProgramId
            )
            .filter(
                AutoProcProgramAttachment.fileName.endswith(
                    input_file_patterns[proc_prog]
                )
            )
        ).one_or_none()

        if not query:
            self.log.error(
                f"Skipping metal_id trigger: No result file found for appid {matching_job.autoProcProgramId}"
            )
            return {"success": True}

        mtz_file_2 = pathlib.Path(query.filePath) / query.fileName
        self.log.info(f"Retrieved mtz file {mtz_file_2} from matching data collection")

        wavelengths = message.get("wavelengths", wavelengths)
        if any(
            wavelength < parameters.dc_info.wavelength for wavelength in wavelengths
        ):
            mtz_file_above = mtz_file_2
            mtz_file_below = mtz_file_1
        else:
            mtz_file_above = mtz_file_1
            mtz_file_below = mtz_file_2

        metal_id_parameters: dict[str, list[Any]] = {
            "dcids": dcids,
            "data": [mtz_file_below.as_posix(), mtz_file_above.as_posix()],
            "scaling_id": [parameters.scaling_id],
            "pdb": pdb_files,
        }

        self.log.debug("Metal_id trigger: Starting")

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = parameters.automatic
        jp["comments"] = parameters.comment
        jp["datacollectionid"] = parameters.dcid
        jp["display_name"] = "metal_id"
        jp["recipe"] = "postprocessing-metal-id"
        self.log.info(jp)
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"metal_id trigger: generated JobID {jobid}")

        jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
        jisp["datacollectionid"] = parameters.dcid
        jisp["start_image"] = parameters.dc_info.startImageNumber
        jisp["end_image"] = (
            parameters.dc_info.startImageNumber + parameters.dc_info.numberOfImages - 1
        )
        for key, values in metal_id_parameters.items():
            for value in values:
                jpp = self.ispyb.mx_processing.get_job_parameter_params()
                jpp["job_id"] = jobid
                jpp["parameter_key"] = key
                jpp["parameter_value"] = value
                jppid = self.ispyb.mx_processing.upsert_job_parameter(
                    list(jpp.values())
                )
                self.log.debug(
                    f"Metal_id trigger: generated JobParameterID {jppid} with {key}={value}"
                )

        jisp["job_id"] = jobid
        jispid = self.ispyb.mx_processing.upsert_job_image_sweep(list(jisp.values()))
        self.log.debug(f"Metal_id trigger: generated JobImageSweepID {jispid}")

        self.log.debug(f"Metal_id trigger: Processing job {jobid} created")

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info(f"Metal_id trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_ep_predict(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        parameters: EPPredictParameters,
        session: sqlalchemy.orm.session.Session,
        **kwargs,
    ):
        if (
            not parameters.diffraction_plan_info
            or not parameters.diffraction_plan_info.anomalousScatterer
        ):
            self.log.info(
                "Skipping ep_predict trigger: no anomalous scatterer specified"
            )
            return {"success": True}

        dcid = parameters.dcid
        query = (
            session.query(DataCollection, Proposal)
            .join(BLSession, BLSession.proposalId == Proposal.proposalId)
            .join(DataCollection, DataCollection.SESSIONID == BLSession.sessionId)
            .filter(DataCollection.dataCollectionId == dcid)
        )
        rows = query.all()
        if not rows:
            self.log.error(
                f"ep_predict trigger failed: no proposal associated with dcid={dcid}"
            )
            return False

        dc, proposal = rows[0]
        if proposal.proposalCode in INDUSTRIAL_CODES:
            self.log.info(
                f"Skipping ep_predict trigger for {proposal.proposalCode} visit"
            )
            return {"success": True}

        jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
        jisp["datacollectionid"] = dcid
        jisp["start_image"] = dc.startImageNumber
        jisp["end_image"] = dc.startImageNumber + dc.numberOfImages - 1

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = parameters.automatic
        jp["comments"] = parameters.comment
        jp["datacollectionid"] = dcid
        jp["display_name"] = "ep_predict"
        jp["recipe"] = "postprocessing-ep-predict"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"ep_predict trigger: generated JobID {jobid}")

        ep_parameters = {
            "program": parameters.program,
            "program_id": parameters.program_id,
            "data": os.fspath(parameters.data),
            "threshold": parameters.threshold,
        }

        for key, value in ep_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(f"ep_predict trigger: generated JobParameterID {jppid}")

        jisp["job_id"] = jobid
        jispid = self.ispyb.mx_processing.upsert_job_image_sweep(list(jisp.values()))
        self.log.debug(f"ep_predict trigger: generated JobImageSweepID {jispid}")

        self.log.debug(f"ep_predict trigger: Processing job {jobid} created")

        message = {
            "parameters": {
                "ispyb_process": jobid,
                "program": parameters.program,
                "data": os.fspath(parameters.data),
                "threshold": parameters.threshold,
            },
            "recipes": [],
        }
        rw.transport.send("processing_recipe", message)

        self.log.info(f"ep_predict trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_mr_predict(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        parameters: MRPredictParameters,
        session: sqlalchemy.orm.session.Session,
        **kwargs,
    ):
        dcid = parameters.dcid

        query = (
            session.query(Proposal)
            .join(BLSession, BLSession.proposalId == Proposal.proposalId)
            .join(DataCollection, DataCollection.SESSIONID == BLSession.sessionId)
            .filter(DataCollection.dataCollectionId == dcid)
        )
        proposal = query.first()
        if not proposal:
            self.log.error(
                f"mr_predict trigger failed: no proposal associated with dcid={dcid}"
            )
            return False
        if proposal.proposalCode in INDUSTRIAL_CODES:
            self.log.info(
                f"Skipping mr_predict trigger for {proposal.proposalCode} visit"
            )
            return {"success": True}

        if not parameters.diffraction_plan_info:
            self.log.info(
                "Skipping mr_predict trigger: diffraction plan information not available"
            )
            return {"success": True}

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = parameters.automatic
        jp["comments"] = parameters.comment
        jp["datacollectionid"] = dcid
        jp["display_name"] = "mr_predict"
        jp["recipe"] = "postprocessing-mr-predict"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"mr_predict trigger: generated JobID {jobid}")

        mr_parameters = {
            "program_id": parameters.program_id,
            "program": parameters.program_id,
            "data": os.fspath(parameters.data),
            "threshold": parameters.threshold,
        }

        for key, value in mr_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(f"mr_predict trigger: generated JobParameterID {jppid}")

        self.log.debug(f"mr_predict trigger: Processing job {jobid} created")

        message = {
            "parameters": {
                "ispyb_process": jobid,
                "program": parameters.program,
                "data": os.fspath(parameters.data),
                "threshold": parameters.threshold,
            },
            "recipes": [],
        }
        rw.transport.send("processing_recipe", message)

        self.log.info(f"mr_predict trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_screen19_mx(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        parameters: Screen19MXParameters,
        session: sqlalchemy.orm.session.Session,
        **kwargs,
    ):
        dcid = parameters.dcid

        if (
            parameters.visit
            and parameters.test_visit
            and parameters.visit != parameters.test_visit
        ):
            self.log.debug(
                f"screen19_mx trigger: processing is enabled only for testing in {parameters.test_visit}"
            )
            return {"success": True}

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = parameters.automatic
        jp["comments"] = parameters.comment
        jp["datacollectionid"] = dcid
        jp["display_name"] = "screen19_mx"
        jp["recipe"] = "postprocessing-screen19-mx"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"screen19_mx trigger: generated JobID {jobid}")

        screen19_parameters = {
            "program_id": parameters.program_id,
            "data": os.fspath(parameters.data),
        }

        for key, value in screen19_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(f"screen19_mx trigger: generated JobParameterID {jppid}")

        self.log.debug(f"screen19_mx trigger: Processing job {jobid} created")

        message = {
            "parameters": {
                "ispyb_process": jobid,
                "data": os.fspath(parameters.data),
            },
            "recipes": [],
        }
        rw.transport.send("processing_recipe", message)

        self.log.info(f"screen19_mx trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_best(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        parameters: BestParameters,
        **kwargs,
    ):
        dcid = parameters.dcid
        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = parameters.automatic
        jp["comments"] = parameters.comment
        jp["datacollectionid"] = dcid
        jp["display_name"] = "best"
        jp["recipe"] = "postprocessing-best"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"best trigger: generated JobID {jobid}")

        best_parameters = {
            "program_id": parameters.program_id,
            "data": os.fspath(parameters.data),
        }

        for key, value in best_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(f"best trigger: generated JobParameterID {jppid}")

        self.log.debug(f"best trigger: Processing job {jobid} created")

        message = {
            "parameters": {"ispyb_process": jobid, "data": os.fspath(parameters.data)},
            "recipes": [],
        }
        rw.transport.send("processing_recipe", message)

        self.log.info(f"best trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_fast_ep(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        parameters: FastEPParameters,
        session: sqlalchemy.orm.session.Session,
        **kwargs,
    ):
        if parameters.automatic:
            if parameters.experiment_type not in (
                "OSC",
                "SAD",
                "MAD",
                "Helical",
                "Metal ID",
            ):
                self.log.info(
                    f"Skipping fast_ep trigger: experiment type {parameters.experiment_type} not supported"
                )
                return {"success": True}

            if (
                not parameters.diffraction_plan_info
                or not parameters.diffraction_plan_info.anomalousScatterer
            ):
                self.log.info(
                    "Skipping fast_ep trigger: no anomalous scatterer specified"
                )
                return {"success": True}

        if isinstance(parameters.mtz, dict):
            query = (
                session.query(
                    AutoProcScaling.autoProcScalingId,
                    AutoProcProgram.processingPrograms,
                    AutoProcProgramAttachment.filePath,
                    AutoProcProgramAttachment.fileName,
                )
                .join(
                    AutoProcScalingHasInt,
                    AutoProcScalingHasInt.autoProcScalingId
                    == AutoProcScaling.autoProcScalingId,
                )
                .join(
                    AutoProcIntegration,
                    AutoProcIntegration.autoProcIntegrationId
                    == AutoProcScalingHasInt.autoProcIntegrationId,
                )
                .join(
                    AutoProcProgram,
                    AutoProcProgram.autoProcProgramId
                    == AutoProcIntegration.autoProcProgramId,
                )
                .join(
                    AutoProcProgramAttachment,
                    AutoProcProgramAttachment.autoProcProgramId
                    == AutoProcProgram.autoProcProgramId,
                )
                .filter(AutoProcScaling.autoProcScalingId == parameters.scaling_id)
            )
            attachments = query.all()
            for _, program_name, filepath, filename in attachments:
                if filename == str(parameters.mtz.get(program_name)):
                    mtzin = pathlib.Path(filepath) / filename
                    break
            else:
                self.log.error(
                    "Skipping mrbump trigger: No input data files found for ScalingId %s",
                    parameters.scaling_id,
                )
                return {"success": True}
        elif isinstance(parameters.mtz, pathlib.Path):
            mtzin = parameters.mtz
        else:
            self.log.error(
                "Skipping fast_ep trigger: Invalid input data type %s",
                type(parameters.mtz),
            )
            return {"success": True}

        dcid = parameters.dcid
        query = session.query(DataCollection).filter(
            DataCollection.dataCollectionId == dcid
        )
        dc = query.one()
        jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
        jisp["datacollectionid"] = dcid
        jisp["start_image"] = dc.startImageNumber
        jisp["end_image"] = dc.startImageNumber + dc.numberOfImages - 1

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = parameters.automatic
        jp["comments"] = parameters.comment
        jp["datacollectionid"] = dcid
        jp["display_name"] = "fast_ep"
        jp["recipe"] = parameters.recipe or "postprocessing-fast-ep"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"fast_ep trigger: generated JobID {jobid}")

        fast_ep_parameters = {
            "data": os.fspath(mtzin),
            "scaling_id": parameters.scaling_id,
        }

        for key, value in fast_ep_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(f"fast_ep trigger: generated JobParameterID {jppid}")

        jisp["job_id"] = jobid
        jispid = self.ispyb.mx_processing.upsert_job_image_sweep(list(jisp.values()))
        self.log.debug(f"fast_ep trigger: generated JobImageSweepID {jispid}")

        self.log.debug(f"fast_ep trigger: Processing job {jobid} created")

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info(f"fast_ep trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_mrbump(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        parameters: MrBumpParameters,
        session: sqlalchemy.orm.session.Session,
        **kwargs,
    ):
        dcid = parameters.dcid
        if not dcid:
            self.log.error("mrbump trigger failed: No DCID specified")
            return False

        if parameters.experiment_type not in ("OSC", "SAD", "MAD", "Metal ID"):
            self.log.info(
                f"Skipping mrbump trigger: experiment type {parameters.experiment_type} not supported"
            )
            return {"success": True}

        if not (parameters.protein_info and parameters.protein_info.sequence):
            self.log.info("Skipping mrbump trigger: sequence information not available")
            return {"success": True}

        if isinstance(parameters.hklin, dict):
            query = (
                session.query(
                    AutoProcScaling.autoProcScalingId,
                    AutoProcProgram.processingPrograms,
                    AutoProcProgramAttachment.filePath,
                    AutoProcProgramAttachment.fileName,
                )
                .join(
                    AutoProcScalingHasInt,
                    AutoProcScalingHasInt.autoProcScalingId
                    == AutoProcScaling.autoProcScalingId,
                )
                .join(
                    AutoProcIntegration,
                    AutoProcIntegration.autoProcIntegrationId
                    == AutoProcScalingHasInt.autoProcIntegrationId,
                )
                .join(
                    AutoProcProgram,
                    AutoProcProgram.autoProcProgramId
                    == AutoProcIntegration.autoProcProgramId,
                )
                .join(
                    AutoProcProgramAttachment,
                    AutoProcProgramAttachment.autoProcProgramId
                    == AutoProcProgram.autoProcProgramId,
                )
                .filter(AutoProcScaling.autoProcScalingId == parameters.scaling_id)
            )
            attachments = query.all()
            for _, program_name, filepath, filename in attachments:
                if filename == str(parameters.hklin.get(program_name)):
                    hklin = pathlib.Path(filepath) / filename
                    break
            else:
                self.log.error(
                    "Skipping mrbump trigger: No input data files found for ScalingId %s",
                    parameters.scaling_id,
                )
                return {"success": True}
        elif isinstance(parameters.hklin, pathlib.Path):
            hklin = parameters.hklin
        else:
            self.log.error(
                "Skipping mrbump trigger: Invalid input data type %s",
                type(parameters.hklin),
            )
            return {"success": True}
        jobids = []

        all_pdb_files = set()
        for pdb_param in parameters.pdb:
            if (fp := pdb_param.filepath) is not None and pathlib.Path(fp).is_file():
                all_pdb_files.add(pdb_param)
        for pdb_files in {(), tuple(all_pdb_files)}:
            jp = self.ispyb.mx_processing.get_job_params()
            jp["automatic"] = parameters.automatic
            jp["comments"] = parameters.comment
            jp["datacollectionid"] = dcid
            jp["display_name"] = "MrBUMP"
            jp["recipe"] = parameters.recipe or "postprocessing-mrbump"
            jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
            jobids.append(jobid)
            self.log.debug(f"mrbump trigger: generated JobID {jobid}")

            mrbump_parameters = {
                "hklin": os.fspath(hklin),
                "scaling_id": parameters.scaling_id,
            }
            if pdb_files:
                mrbump_parameters["dophmmer"] = "False"
                mrbump_parameters["mdlunmod"] = "True"

            for key, value in mrbump_parameters.items():
                jpp = self.ispyb.mx_processing.get_job_parameter_params()
                jpp["job_id"] = jobid
                jpp["parameter_key"] = key
                jpp["parameter_value"] = value
                jppid = self.ispyb.mx_processing.upsert_job_parameter(
                    list(jpp.values())
                )
                self.log.debug(f"mrbump trigger: generated JobParameterID {jppid}")

            for pdb_file in pdb_files:
                if not pdb_file.filepath:
                    # presumably just a pdb code provided
                    continue
                filepath = pathlib.Path(pdb_file.filepath)
                if pdb_file.source == "AlphaFold":
                    trimmed = filepath.with_name(
                        filepath.stem + "_trimmed" + filepath.suffix
                    )
                    trim_pdb_bfactors(
                        os.fspath(filepath),
                        os.fspath(trimmed),
                        atom_selection="bfactor > 70",
                        set_b_iso=20,
                    )
                    filepath = trimmed

                jpp = self.ispyb.mx_processing.get_job_parameter_params()
                jpp["job_id"] = jobid
                jpp["parameter_key"] = "localfile"
                jpp["parameter_value"] = os.fspath(filepath)
                jppid = self.ispyb.mx_processing.upsert_job_parameter(
                    list(jpp.values())
                )
                self.log.debug(f"mrbump trigger: generated JobParameterID {jppid}")

            self.log.debug(f"mrbump trigger: Processing job {jobid} created")

            message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
            rw.transport.send("processing_recipe", message)

            self.log.info(f"mrbump trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobids}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_big_ep_launcher(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        parameters: BigEPLauncherParameters,
        session,
        **kwargs,
    ):
        query = (
            session.query(Proposal)
            .join(BLSession, BLSession.proposalId == Proposal.proposalId)
            .join(DataCollection, DataCollection.SESSIONID == BLSession.sessionId)
            .filter(DataCollection.dataCollectionId == parameters.dcid)
        )
        proposal = query.first()
        if proposal.proposalCode in INDUSTRIAL_CODES:
            self.log.info(f"Skipping big_ep trigger for {proposal.proposalCode} visit")
            return {"success": True}

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = parameters.automatic
        jp["comments"] = parameters.comment
        jp["datacollectionid"] = parameters.dcid
        jp["display_name"] = parameters.pipeline
        jp["recipe"] = parameters.recipe or "postprocessing-big-ep-launcher"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"big_ep_launcher trigger: generated JobID {jobid}")

        try:
            program_id = parameters.program_id
        except (TypeError, ValueError):
            self.log.error(
                "big_ep_launcher trigger failed: Invalid program_id specified"
            )
            return False
        try:
            scaling_id = parameters.scaling_id
        except (TypeError, ValueError):
            self.log.error(
                "big_ep_launcher trigger failed: Invalid scaling_id specified"
            )
            return False
        big_ep_parameters = {
            "pipeline": parameters.pipeline,
            "program_id": program_id,
            "scaling_id": scaling_id,
            "data": os.fspath(parameters.data),
            "path_ext": parameters.path_ext,
            "shelxc_path": os.fspath(parameters.shelxc_path),
            "fast_ep_path": os.fspath(parameters.fast_ep_path),
            "upstream_source": parameters.upstream_source,
        }

        for key, value in big_ep_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(f"big_ep_launcher trigger: generated JobParameterID {jppid}")

        self.log.debug(f"big_ep_launcher trigger: Processing job {jobid} created")

        message = {
            "recipes": [],
            "parameters": {"ispyb_process": jobid},
        }
        rw.transport.send("processing_recipe", message)

        self.log.info(f"big_ep_launcher trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_big_ep(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        parameters: BigEPParameters,
        parameter_map: Mapping,
        session: sqlalchemy.orm.session.Session,
        **kwargs,
    ):
        bigep_path_ext = {
            "autoPROC": "autoPROC/ap-run",
            "autoPROC+STARANISO": "autoPROC-STARANISO/ap-run",
            "xia2 3dii": "xia2/3dii-run",
            "xia2 dials": "xia2/dials-run",
            "xia2 3dii (multi)": "multi-xia2/3dii",
            "xia2 dials (multi)": "multi-xia2/dials",
            "xia2.multiplex": "xia2.multiplex",
        }
        if parameters.experiment_type not in (
            "OSC",
            "SAD",
            "MAD",
            "Helical",
            "Metal ID",
        ):
            self.log.info(
                f"Skipping big_ep trigger: experiment type {parameters.experiment_type} not supported"
            )
            return {"success": True}

        if not (
            parameters.diffraction_plan_info
            and parameters.diffraction_plan_info.anomalousScatterer
        ):
            self.log.info("Skipping big_ep trigger: No anomalous scatterer specified")
            return {"success": True}

        dcid = parameters.dcid
        query = (
            session.query(Proposal, BLSession)
            .join(BLSession, BLSession.proposalId == Proposal.proposalId)
            .join(DataCollection, DataCollection.SESSIONID == BLSession.sessionId)
            .filter(DataCollection.dataCollectionId == dcid)
        )
        proposal, blsession = query.first()
        if proposal.proposalCode in INDUSTRIAL_CODES:
            self.log.info(f"Skipping big_ep trigger for {proposal.proposalCode} visit")
            return {"success": True}

        class BigEPParams(pydantic.BaseModel):
            program_id: int = pydantic.Field(gt=0)
            data: pathlib.Path
            scaled_unmerged_mtz: pathlib.Path
            path_ext: str = pydantic.Field(
                default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S")
            )

        if parameters.scaling_id:
            query = (
                session.query(
                    AutoProcScaling.autoProcScalingId,
                    AutoProcProgram.autoProcProgramId,
                    AutoProcProgram.processingPrograms,
                    AutoProcProgramAttachment.filePath,
                    AutoProcProgramAttachment.fileName,
                )
                .join(
                    AutoProcScalingHasInt,
                    AutoProcScalingHasInt.autoProcScalingId
                    == AutoProcScaling.autoProcScalingId,
                )
                .join(
                    AutoProcIntegration,
                    AutoProcIntegration.autoProcIntegrationId
                    == AutoProcScalingHasInt.autoProcIntegrationId,
                )
                .join(
                    AutoProcProgram,
                    AutoProcProgram.autoProcProgramId
                    == AutoProcIntegration.autoProcProgramId,
                )
                .join(
                    AutoProcProgramAttachment,
                    AutoProcProgramAttachment.autoProcProgramId
                    == AutoProcProgram.autoProcProgramId,
                )
                .filter(AutoProcScaling.autoProcScalingId == parameters.scaling_id)
            )
            attachments = query.all()
            big_ep_params = {}
            for app in attachments:
                if (
                    parameters.automatic
                    and blsession.beamLineName == "i23"
                    and "multi" not in app.processingPrograms
                ):
                    self.log.info(
                        f"Skipping big_ep trigger for {app.processingPrograms} data on i23"
                    )
                    return {"success": True}
                big_ep_params["program_id"] = app.autoProcProgramId
                try:
                    big_ep_params["path_ext"] = bigep_path_ext[app.processingPrograms]
                except Exception:
                    pass

                for input_file in ("data", "scaled_unmerged_mtz"):
                    processing_params = parameter_map.get(app.processingPrograms, {})
                    input_filename = getattr(
                        parameters, input_file
                    ) or processing_params.get(input_file)
                    if pathlib.Path(input_filename).is_file():
                        big_ep_params[input_file] = pathlib.Path(input_filename)
                    else:
                        input_filename = pathlib.Path(app.filePath) / input_filename
                        app_file = pathlib.Path(app.filePath) / app.fileName
                        if re.search(str(input_filename), str(app_file)):
                            big_ep_params[input_file] = app_file
            big_ep_params = BigEPParams(
                **ChainMapWithReplacement(
                    big_ep_params,
                    substitutions=rw.environment,
                )
            )
        else:
            self.log.error("big_ep trigger failed: No scaling_id value specified")
            return False

        if parameters.spacegroup:
            big_ep_params.path_ext += "-" + parameters.spacegroup

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = parameters.automatic
        jp["comments"] = parameters.comment
        jp["datacollectionid"] = dcid
        jp["display_name"] = "big_ep"
        jp["recipe"] = parameters.recipe or "postprocessing-big-ep"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"big_ep trigger: generated JobID {jobid}")

        pattern_scaled_unmerged = str(
            getattr(parameters, "scaled_unmerged_mtz")
            or processing_params.get("scaled_unmerged_mtz")
        )
        big_ep_parameters = {
            "program_id": big_ep_params.program_id,
            "scaling_id": parameters.scaling_id,
            "data": str(big_ep_params.data.resolve()),
            "scaled_unmerged_mtz": str(
                big_ep_params.scaled_unmerged_mtz.resolve().parent
                / pattern_scaled_unmerged
            ),
            "upstream_source": parameters.upstream_source,
        }

        for key, value in big_ep_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(f"big_ep trigger: generated JobParameterID {jppid}")

        self.log.debug(f"big_ep trigger: Processing job {jobid} created")

        message = {
            "parameters": {
                "ispyb_process": jobid,
                "scaling_id": parameters.scaling_id,
                "data": big_ep_parameters["data"],
                "scaled_unmerged_mtz": big_ep_parameters["scaled_unmerged_mtz"],
                "path_ext": big_ep_params.path_ext,
                "force": not parameters.automatic or False,
            },
            "recipes": [],
        }
        rw.transport.send("processing_recipe", message)

        self.log.info("big_ep triggered")

        return {"success": True, "return_value": None}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_multiplex(
        self,
        rw: RecipeWrapper,
        message: Dict,
        parameters: MultiplexParameters,
        session: sqlalchemy.orm.session.Session,
        transaction: int,
        **kwargs,
    ):
        """Trigger a xia2.multiplex job for a given data collection.

        Identify all successful autoprocessing xia2-dials results for a group of related
        data collections. A xia2.multiplex job will be triggered using as input the
        integrated.expt and integrated.refl files generated by the xia2-dials jobs for
        each related data collection.

        If this multiplex job was triggered with a spacegroup parameter then only use
        xia2-dials autoprocessing results that were themselves run with a spacegroup
        parameter. Else only use those results that weren't run with a space group
        parameter.

        If any of the xia2-dials jobs are still running (or yet to start), we
        checkpoint the current message with a delay, where the delay is given by

            delay = backoff-delay * backoff-multiplier ** ntry

        If any xia2-dials jobs are still running after backoff-max-try iterations,
        then a multiplex will be triggered with whatever remaining related results
        are available.

        Multiple groups of `related_dcids` may be provided, in which case a separate
        xia2.multiplex job will be triggered for each group of related dcids.

        New ProcessingJob, ProcessingJobImageSweep and ProcessingJobParameter entries
        will be created, and the resulting list of processingJobIds will be sent to
        the `processing_recipe` queue.

        If clustering algorithm is enabled, skip triggering multiplex if new related dcid
        values are added into all defined sample groups to run multiplex only once when
        all samples in one of the groups have been collected. When running multiplex
        only include results from datasets collected prior to the current one in all
        sample groups.

        Recipe parameters:
        - target: set this to "multiplex"
        - dcid: the dataCollectionId for the given data collection
        - comment: a comment to be stored in the ProcessingJob.comment field
        - automatic: boolean value passed to ProcessingJob.automatic field
        - ispyb_parameters: a dictionary of ispyb_reprocessing_parameters set in the
            parent xia2-dials processing job
        - related_dcids: a list of groups of related data collection ids. Each item in
            the list should be a dictionary containing at a minimum the item "dcids"
            pointing to the list of related data collection ids in that group.
            Optionally the items "sample_id" or "sample_group_id" can be present,
            pointing to a BLSample or BLSampleGroup entry corresponding to the group
            of related data collections
        - backoff-delay: base delay (in seconds) for exponential backoff in the event
            that one or more xia2-dials processing job is still running or yet to start
        - backoff-max-try: the number of times that a message re-sent before giving up
        - backoff-multiplier: the multiplier for exponential backoff

        Example recipe parameters:
        {
            "target": "multiplex",
            "dcid": 123456,
            "wavelength": "1.03936",
            "comment": "xia2.multiplex triggered by automatic xia2-dials",
            "automatic": True,
            "ispyb_parameters": {"spacegroup": "P421212"},
            "related_dcids": [
                {
                    "dcids": [123451, 123452, 123453],
                    "sample_group_id": 456,
                    "name": "sample_group_foo",
                },
                {
                    "dcids": [123453],
                    "sample_id": 234,
                    "name": "sample_bar",
                }
            ],
            "backoff-delay": 8, # default
            "backoff-max-try": 10, # default
            "backoff-multiplier": 2, # default
        }
        """
        dcid = parameters.dcid
        program_id = parameters.program_id
        parameters.recipe = "postprocessing-xia2-multiplex"

        # Take related dcids from recipe in preference or checkpointed message
        if isinstance(related_dcid_group := message.get("related_dcid_group"), list):
            # Checkpointed message has dcid group with still running jobs
            related_dcids = [RelatedDCIDs(**el) for el in related_dcid_group]
        else:
            # Initial call of multiplex trigger
            related_dcids = parameters.related_dcids
        self.log.info(f"related_dcids={related_dcids}")

        if not related_dcids:
            self.log.debug(f"No related_dcids for dcid={dcid}")
            return {"success": True}

        self.log.debug(f"related_dcids for dcid={dcid}: {related_dcids}")
        # Check if we have any new data collections added to any sample group
        # to decide if we need to processed triggering multiplex.
        # Run multiplex only once when processing for all samples in the group have been collected.
        if parameters.use_clustering and parameters.program_id:
            # Get currnent list of data collections for all samples in the sample groups
            _, ispyb_info = dlstbx.ispybtbx.ispyb_filter(
                {}, {"ispyb_dcid": dcid}, session
            )
            ispyb_related_dcids = ispyb_info.get("ispyb_related_dcids", [])
            beamline = ispyb_info.get("ispyb_beamline", "")
            visit = ispyb_info.get("ispyb_visit", "")
            if beamline in parameters.use_clustering or any(
                el in visit for el in parameters.use_clustering
            ):
                parameters.recipe = "postprocessing-xia2-multiplex-clustering"
                # If we have a sample group that doesn't have any new data collections,
                # proceed with triggering multiplex for all sample groups
                if all(max(el.get("dcids", [])) > dcid for el in ispyb_related_dcids):
                    added_dcids = []
                    for el in ispyb_related_dcids:
                        added_dcids.extend([d for d in el.get("dcids", []) if d > dcid])
                    # Check if there are xia2 dials jobs that were triggered on any new
                    # data collections after current multiplex job was triggered
                    min_start_time = datetime.now() - timedelta(hours=12)
                    query = (
                        (
                            session.query(
                                AutoProcProgram, ProcessingJob.dataCollectionId
                            ).join(
                                ProcessingJob,
                                ProcessingJob.processingJobId
                                == AutoProcProgram.processingJobId,
                            )
                        )
                        .filter(ProcessingJob.dataCollectionId.in_(added_dcids))
                        .filter(ProcessingJob.automatic == True)  # noqa E712
                        .filter(AutoProcProgram.processingPrograms == "xia2 dials")
                        .filter(AutoProcProgram.autoProcProgramId > program_id)  # noqa E711
                        .filter(AutoProcProgram.recordTimeStamp > min_start_time)  # noqa E711
                    )
                    # Abort triggering multiplex if we have xia2 dials running on any subsequent
                    # data collection in all sample groups
                    if triggered_processing_job := query.first():
                        self.log.info(
                            f"Aborting multiplex trigger for dcid {dcid} as processing job has been started for dcid {triggered_processing_job.dataCollectionId}"
                        )
                        return {"success": True}

        # Calculate message delay for exponential backoff in case a processing
        # program for a related data collection is still running, in which case
        # we checkpoint with the calculated message delay
        status = {
            "ntry": 0,
        }
        if isinstance(message, dict):
            status.update(message.get("trigger-status", {}))
        message_delay = int(
            parameters.backoff_delay * parameters.backoff_multiplier ** status["ntry"]
        )
        status["ntry"] += 1
        self.log.debug(f"dcid={dcid}\nmessage_delay={message_delay}\n{status}")

        multiplex_job_dcids: list[set[int]] = []
        jobids = []

        for group in related_dcids:
            self.log.debug(f"group: {group}")
            # Select only those dcids that were collected before the triggering dcid
            dcids = [d for d in group.dcids if d < dcid]

            # Add the current dcid at the beginning of the list
            dcids.insert(0, dcid)

            if len(dcids) == 1:
                self.log.info(
                    f"Skipping xia2.multiplex trigger: no related dcids for dcid={dcid} group={group}"
                )
                continue
            self.log.info(f"xia2.multiplex trigger: found dcids: {dcids}")

            # Check for any processing jobs that are yet to finish (or fail)
            min_start_time = datetime.now() - timedelta(hours=24)
            query = (
                (
                    session.query(AutoProcProgram, ProcessingJob.dataCollectionId).join(
                        ProcessingJob,
                        ProcessingJob.processingJobId
                        == AutoProcProgram.processingJobId,
                    )
                )
                .filter(ProcessingJob.dataCollectionId.in_(dcids))
                .filter(ProcessingJob.automatic == True)  # noqa E712
                .filter(AutoProcProgram.processingPrograms == "xia2 dials")
                .filter(ProcessingJob.recordTimestamp > min_start_time)  # noqa E711
                .filter(
                    or_(
                        AutoProcProgram.processingStatus == None,  # noqa E711
                        AutoProcProgram.processingStartTime == None,  # noqa E711
                    )
                )
            )

            # If there are any running (or yet to start) jobs, then checkpoint with delay
            waiting_processing_jobs = query.all()
            if n_waiting_processing_jobs := len(waiting_processing_jobs):
                self.log.info(
                    f"Waiting on {n_waiting_processing_jobs} processing jobs for {dcid=}"
                )
                waiting_dcids = [
                    row.dataCollectionId for row in waiting_processing_jobs
                ]
                waiting_appids = [
                    row.AutoProcProgram.autoProcProgramId
                    for row in waiting_processing_jobs
                ]
                if status["ntry"] >= parameters.backoff_max_try:
                    # Give up waiting for this program to finish and trigger
                    # multiplex with remaining related results are available
                    self.log.info(
                        f"max-try exceeded, giving up waiting for related processings for dcids {waiting_dcids}\n"
                    )
                else:
                    # Send results to myself for next round of processing
                    self.log.debug(
                        f"Waiting for dcids={waiting_dcids}\nappids={waiting_appids}"
                    )
                    rw.checkpoint(
                        {
                            "trigger-status": status,
                            "related_dcid_group": [
                                group.model_dump(),
                            ],
                        },
                        delay=message_delay,
                        transaction=transaction,
                    )
                    continue

            query = (
                (
                    session.query(
                        DataCollection,
                        AutoProcProgram,
                        ProcessingJob,
                    )
                    .join(
                        AutoProcIntegration,
                        AutoProcIntegration.dataCollectionId
                        == DataCollection.dataCollectionId,
                    )
                    .join(
                        AutoProcProgram,
                        AutoProcProgram.autoProcProgramId
                        == AutoProcIntegration.autoProcProgramId,
                    )
                    .join(
                        ProcessingJob,
                        ProcessingJob.processingJobId
                        == AutoProcProgram.processingJobId,
                    )
                    .join(AutoProcProgram.AutoProcProgramAttachments)
                )
                .filter(DataCollection.dataCollectionId.in_(dcids))
                .filter(ProcessingJob.automatic == True)  # noqa E712
                .filter(AutoProcProgram.processingPrograms == "xia2 dials")
                .filter(AutoProcProgram.processingStatus == 1)
                .filter(
                    (
                        AutoProcProgramAttachment.fileName.endswith(".expt")
                        | AutoProcProgramAttachment.fileName.endswith(".refl")
                    )
                    & ~AutoProcProgramAttachment.fileName.contains("_scaled.")
                )
                .options(
                    contains_eager(AutoProcProgram.AutoProcProgramAttachments),
                    joinedload(ProcessingJob.ProcessingJobParameters),
                    Load(DataCollection).load_only(
                        DataCollection.dataCollectionId,
                        DataCollection.wavelength,
                        raiseload=True,
                    ),
                )
                .populate_existing()
            )

            dcids = []
            data_files = []
            for dc, app, pj in query.all():
                # Select only those dcids at the same wavelength as the triggering dcid
                if not dc.wavelength:
                    self.log.debug(
                        f"Discarding appid {app.autoProcProgramId} (no wavelength information)"
                    )
                    continue
                if (
                    parameters.wavelength
                    and abs(dc.wavelength - parameters.wavelength)
                    > parameters.wavelength_tolerance
                ):
                    self.log.debug(
                        f"Discarding appid {app.autoProcProgramId} (wavelength does not match input):\n"
                        f"    {dc.wavelength} != {parameters.wavelength} (tolerance={parameters.wavelength_tolerance}"
                    )
                    continue

                # If this multiplex job was triggered with a spacegroup parameter
                # then only use xia2-dials autoprocessing results that were
                # themselves run with a spacegroup parameter. Else only use those
                # results that weren't run with a space group parameter
                job_spacegroup_param = None
                for param in pj.ProcessingJobParameters:
                    self.log.debug(f"{param.parameterKey}: {param.parameterValue}")
                    if param.parameterKey == "spacegroup":
                        job_spacegroup_param = param.parameterValue
                        break
                if parameters.spacegroup and (
                    not job_spacegroup_param
                    or job_spacegroup_param != parameters.spacegroup
                ):
                    self.log.debug(f"Discarding appid {app.autoProcProgramId}")
                    continue
                elif job_spacegroup_param and not parameters.spacegroup:
                    self.log.debug(f"Discarding appid {app.autoProcProgramId}")
                    continue

                self.log.debug(f"Using appid {app.autoProcProgramId}")
                attachments = [
                    str(pathlib.Path(att.filePath) / att.fileName)
                    for att in app.AutoProcProgramAttachments
                ]
                self.log.debug(
                    f"Found the following files for appid {app.autoProcProgramId}:\n{', '.join(attachments)}"
                )
                if len(attachments) % 2:
                    self.log.warning(
                        f"Expected to find an even number of data files for appid {app.autoProcProgramId} (found {len(attachments)})"
                    )
                    continue
                if len(attachments) != 2:
                    f"Skipping xia2.multiplex trigger: Found {len(attachments)} attachments, expected only two for dcid={dcid} group={group}"
                    continue

                dcids.append(dc.dataCollectionId)
                data_files.append(attachments)

            if not any(data_files):
                self.log.info(
                    f"Skipping xia2.multiplex trigger: no related data files found for dcid={dcid} group={group}"
                )
                continue

            self.log.info(data_files)
            if len(data_files) <= 1:
                self.log.info(
                    f"Skipping xia2.multiplex trigger: not enough related data files found for dcid={dcid} group={group}"
                )
                continue

            set_dcids = set(dcids)
            self.log.debug(set_dcids)
            self.log.debug(multiplex_job_dcids)
            # Check if upstream dials job has succeeded when we run multiplex per data collection
            if ("clustering" not in parameters.recipe) and (dcid not in set_dcids):
                self.log.info(
                    f"Skipping xia2.multiplex trigger: upstream dials job failed for dcid={dcid} group={group}"
                )
                continue
            if set_dcids in multiplex_job_dcids:
                continue
            if dcid in set_dcids and len(set_dcids) == 1:
                self.log.info(
                    f"Skipping xia2.multiplex trigger: no related processing results found for dcid={dcid} group={group}"
                )
                continue
            multiplex_job_dcids.append(set_dcids)

            jp = self.ispyb.mx_processing.get_job_params()
            jp["automatic"] = parameters.automatic
            jp["comments"] = parameters.comment
            jp["datacollectionid"] = dcid
            jp["display_name"] = "xia2.multiplex"
            jp["recipe"] = parameters.recipe
            self.log.info(jp)
            jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
            jobids.append(jobid)
            self.log.debug(f"xia2.multiplex trigger: generated JobID {jobid}")

            query = (
                session.query(DataCollection)
                .filter(DataCollection.dataCollectionId.in_(dcids))
                .options(
                    Load(DataCollection).load_only(
                        DataCollection.dataCollectionId,
                        DataCollection.wavelength,
                        DataCollection.startImageNumber,
                        DataCollection.numberOfImages,
                        raiseload=True,
                    )
                )
            )
            for dc in query.all():
                jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
                jisp["datacollectionid"] = dc.dataCollectionId
                jisp["start_image"] = dc.startImageNumber
                jisp["end_image"] = dc.startImageNumber + dc.numberOfImages - 1

                jisp["job_id"] = jobid
                jispid = self.ispyb.mx_processing.upsert_job_image_sweep(
                    list(jisp.values())
                )
                self.log.debug(
                    f"xia2.multiplex trigger: generated JobImageSweepID {jispid}"
                )

            job_parameters: list[tuple[str, str]] = [
                ("data", ";".join(files)) for files in data_files
            ]
            if group.sample_id:
                job_parameters.append(("sample_id", str(group.sample_id)))
            else:
                job_parameters.append(("sample_group_id", str(group.sample_group_id)))
            if parameters.spacegroup:
                job_parameters.append(("spacegroup", parameters.spacegroup))
            if (
                parameters.diffraction_plan_info
                and parameters.diffraction_plan_info.anomalousScatterer
            ):
                job_parameters.extend(
                    [
                        ("anomalous", "true"),
                        ("absorption_level", "high"),
                    ]
                )
            if "clustering" in parameters.recipe:
                job_parameters.extend(
                    [
                        ("clustering.method", "coordinate"),
                        ("clustering.output_clusters", "true"),
                    ]
                )
            for k, v in job_parameters:
                jpp = self.ispyb.mx_processing.get_job_parameter_params()
                jpp["job_id"] = jobid
                jpp["parameter_key"] = k
                jpp["parameter_value"] = v
                jppid = self.ispyb.mx_processing.upsert_job_parameter(
                    list(jpp.values())
                )
                self.log.debug(
                    f"xia2.multiplex trigger generated JobParameterID {jppid} with {k}={v}",
                )

            message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
            rw.transport.send("processing_recipe", message)

            self.log.info(f"xia2.multiplex trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobids}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_xia2_ssx_reduce(
        self,
        rw: RecipeWrapper,
        message: Dict,
        parameters: Xia2SsxReduceParameters,
        session: sqlalchemy.orm.session.Session,
        transaction: int,
        **kwargs,
    ):
        """Trigger a xia2.ssx_reduce job for a given data collection.

        Identify all successful autoprocessing xia2.ssx results for a group of related
        data collections. A xia2.ssx_reduce job will be triggered using as input the
        integrated.expt and integrated.refl files generated by the xia2.ssx jobs for
        each related data collection.

        If any of the xia2.ssx jobs are still running (or yet to start), we
        checkpoint the current message with a delay, where the delay is given by

            delay = backoff-delay * backoff-multiplier ** ntry

        If any xia2.ssx jobs are still running after backoff-max-try iterations,
        then a multiplex will be triggered with whatever remaining related results
        are available.

        New ProcessingJob, ProcessingJobImageSweep and ProcessingJobParameter entries
        will be created, and the resulting list of processingJobIds will be sent to
        the `processing_recipe` queue.
        """
        dcid = parameters.dcid

        # Take related dcids from recipe in preference
        related_dcids = parameters.related_dcids
        self.log.info(f"related_dcids={related_dcids}")

        if not related_dcids:
            self.log.debug(f"No related_dcids for dcid={dcid}")
            return {"success": True}

        self.log.debug(f"related_dcids for dcid={dcid}: {related_dcids}")

        # Calculate message delay for exponential backoff in case a processing
        # program for a related data collection is still running, in which case
        # we checkpoint with the calculated message delay
        status = {
            "ntry": 0,
        }
        if isinstance(message, dict):
            status.update(message.get("trigger-status", {}))
        message_delay = (
            parameters.backoff_delay * parameters.backoff_multiplier ** status["ntry"]
        )
        status["ntry"] += 1
        self.log.debug(f"dcid={dcid}\nmessage_delay={message_delay}\n{status}")

        ssx_reduce_job_dcids: list[set[int]] = []
        jobids = []

        for group in related_dcids:
            self.log.debug(f"group: {group}")
            # Select only those dcids that were collected before the triggering dcid
            dcids = [d for d in group.dcids if d < dcid]

            # Add the current dcid at the beginning of the list
            dcids.insert(0, dcid)

            if len(dcids) == 1:
                self.log.info(
                    f"Skipping xia2.ssx_reduce trigger: no related dcids for dcid={dcid} group={group}"
                )
                continue
            self.log.info(f"xia2.ssx_reduce trigger: found dcids: {dcids}")

            # Check for any processing jobs that are yet to finish (or fail)
            query = (
                (
                    session.query(AutoProcProgram, ProcessingJob.dataCollectionId).join(
                        ProcessingJob,
                        ProcessingJob.processingJobId
                        == AutoProcProgram.processingJobId,
                    )
                )
                .filter(ProcessingJob.dataCollectionId.in_(dcids))
                .filter(ProcessingJob.automatic == True)  # noqa E712
                .filter(AutoProcProgram.processingPrograms == "xia2.ssx")
                .filter(
                    or_(
                        AutoProcProgram.processingStatus == None,  # noqa E711
                        AutoProcProgram.processingStartTime == None,  # noqa E711
                    )
                )
            )

            # If there are any running (or yet to start) jobs, then checkpoint with delay
            waiting_processing_jobs = query.all()
            if n_waiting_processing_jobs := len(waiting_processing_jobs):
                self.log.info(
                    f"Waiting on {n_waiting_processing_jobs} processing jobs for {dcid=}"
                )
                waiting_dcids = [
                    row.dataCollectionId for row in waiting_processing_jobs
                ]
                waiting_appids = [
                    row.AutoProcProgram.autoProcProgramId
                    for row in waiting_processing_jobs
                ]
                if status["ntry"] >= parameters.backoff_max_try:
                    # Give up waiting for this program to finish and trigger
                    # multiplex with remaining related results are available
                    self.log.info(
                        f"max-try exceeded, giving up waiting for related processings for dcids {waiting_dcids}\n"
                    )
                else:
                    # Send results to myself for next round of processing
                    self.log.debug(
                        f"Waiting for dcids={waiting_dcids}\nappids={waiting_appids}"
                    )
                    rw.checkpoint(
                        {"trigger-status": status},
                        delay=message_delay,
                        transaction=transaction,
                    )
                    return {"success": True}

            query = (
                (
                    session.query(
                        DataCollection,
                        AutoProcProgram,
                        ProcessingJob,
                    )
                    .join(
                        AutoProcIntegration,
                        AutoProcIntegration.dataCollectionId
                        == DataCollection.dataCollectionId,
                    )
                    .join(
                        AutoProcProgram,
                        AutoProcProgram.autoProcProgramId
                        == AutoProcIntegration.autoProcProgramId,
                    )
                    .join(
                        ProcessingJob,
                        ProcessingJob.processingJobId
                        == AutoProcProgram.processingJobId,
                    )
                    .join(AutoProcProgram.AutoProcProgramAttachments)
                )
                .filter(DataCollection.dataCollectionId.in_(dcids))
                .filter(ProcessingJob.automatic == True)  # noqa E712
                .filter(AutoProcProgram.processingPrograms == "xia2.ssx")
                .filter(AutoProcProgram.processingStatus == 1)
                .filter(
                    (
                        AutoProcProgramAttachment.fileName.endswith(".expt")
                        | AutoProcProgramAttachment.fileName.endswith(".refl")
                    )
                    & AutoProcProgramAttachment.fileName.startswith("integrated")
                )
                .options(
                    contains_eager(AutoProcProgram.AutoProcProgramAttachments),
                    joinedload(ProcessingJob.ProcessingJobParameters),
                    Load(DataCollection).load_only(
                        DataCollection.dataCollectionId,
                        DataCollection.wavelength,
                        raiseload=True,
                    ),
                )
                .populate_existing()
            )

            dcids = []
            data_files = []
            for dc, app, pj in query.all():
                # Select only those dcids at the same wavelength as the triggering dcid
                if (
                    parameters.wavelength
                    and abs(dc.wavelength - parameters.wavelength)
                    > parameters.wavelength_tolerance
                ):
                    self.log.debug(
                        f"Discarding appid {app.autoProcProgramId} (wavelength does not match input):\n"
                        f"    {dc.wavelength} != {parameters.wavelength} (tolerance={parameters.wavelength_tolerance}"
                    )
                    continue

                # If this multiplex job was triggered with a spacegroup parameter
                # then only use xia2-dials autoprocessing results that were
                # themselves run with a spacegroup parameter. Else only use those
                # results that weren't run with a space group parameter
                job_spacegroup_param = None
                for param in pj.ProcessingJobParameters:
                    self.log.debug(f"{param.parameterKey}: {param.parameterValue}")
                    if param.parameterKey == "spacegroup":
                        job_spacegroup_param = param.parameterValue
                        break
                if parameters.spacegroup and (
                    not job_spacegroup_param
                    or job_spacegroup_param != parameters.spacegroup
                ):
                    self.log.debug(f"Discarding appid {app.autoProcProgramId}")
                    continue
                elif job_spacegroup_param and not parameters.spacegroup:
                    self.log.debug(f"Discarding appid {app.autoProcProgramId}")
                    continue

                self.log.debug(f"Using appid {app.autoProcProgramId}")
                attachments = [
                    str(pathlib.Path(att.filePath) / att.fileName)
                    for att in app.AutoProcProgramAttachments
                ]
                self.log.debug(
                    f"Found the following files for appid {app.autoProcProgramId}:\n{', '.join(attachments)}"
                )
                if len(attachments) % 2:
                    self.log.warning(
                        f"Expected to find an even number of data files for appid {app.autoProcProgramId} (found {len(attachments)})"
                    )
                    continue
                if len(attachments) >= 2:
                    dcids.append(dc.dataCollectionId)
                    data_files.extend(attachments)

            if not any(data_files):
                self.log.info(
                    f"Skipping xia2.ssx_reduce trigger: no related data files found for dcid={dcid} group={group}"
                )
                continue

            self.log.info(data_files)
            if len(data_files) <= 2:
                self.log.info(
                    f"Skipping xia2.ssx_reduce trigger: not enough related data files found for dcid={dcid} group={group}"
                )
                continue

            self.log.debug(set(dcids))
            self.log.debug(ssx_reduce_job_dcids)
            if set(dcids) in ssx_reduce_job_dcids:
                continue
            ssx_reduce_job_dcids.append(set(dcids))

            jp = self.ispyb.mx_processing.get_job_params()
            jp["automatic"] = parameters.automatic
            jp["comments"] = parameters.comment
            jp["datacollectionid"] = dcid
            jp["display_name"] = "xia2.ssx_reduce"
            jp["recipe"] = "postprocessing-xia2-ssx-reduce"
            self.log.info(jp)
            jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
            jobids.append(jobid)
            self.log.debug(f"xia2.ssx_reduce trigger: generated JobID {jobid}")

            query = (
                session.query(DataCollection)
                .filter(DataCollection.dataCollectionId.in_(dcids))
                .options(
                    Load(DataCollection).load_only(
                        DataCollection.dataCollectionId,
                        DataCollection.wavelength,
                        DataCollection.startImageNumber,
                        DataCollection.numberOfImages,
                        raiseload=True,
                    )
                )
            )
            for dc in query.all():
                jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
                jisp["datacollectionid"] = dc.dataCollectionId
                jisp["start_image"] = dc.startImageNumber
                jisp["end_image"] = dc.startImageNumber + dc.numberOfImages - 1

                jisp["job_id"] = jobid
                jispid = self.ispyb.mx_processing.upsert_job_image_sweep(
                    list(jisp.values())
                )
                self.log.debug(
                    f"xia2.ssx_reduce trigger: generated JobImageSweepID {jispid}"
                )

            data_files = sorted(data_files)
            # group into pairs
            data_file_pairs = [
                data_files[i : i + 2] for i in range(0, len(data_files), 2)
            ]
            job_parameters: list[tuple[str, str]] = [
                ("data", ";".join(files)) for files in data_file_pairs
            ]
            if group.sample_id:
                job_parameters.append(("sample_id", str(group.sample_id)))
            else:
                job_parameters.append(("sample_group_id", str(group.sample_group_id)))
            if parameters.spacegroup:
                job_parameters.append(("spacegroup", parameters.spacegroup))
            # if (
            #     parameters.diffraction_plan_info
            #     and parameters.diffraction_plan_info.anomalousScatterer
            # ):
            #     job_parameters.extend(
            #         [
            #             ("anomalous", "true"),
            #             ("absorption_level", "high"),
            #         ]
            #     )
            for k, v in job_parameters:
                jpp = self.ispyb.mx_processing.get_job_parameter_params()
                jpp["job_id"] = jobid
                jpp["parameter_key"] = k
                jpp["parameter_value"] = v
                jppid = self.ispyb.mx_processing.upsert_job_parameter(
                    list(jpp.values())
                )
                self.log.debug(
                    f"xia2.ssx_reduce trigger generated JobParameterID {jppid} with {k}={v}",
                )

            message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
            rw.transport.send("processing_recipe", message)

            self.log.info(f"xia2.ssx_reduce trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobids}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_alphafold(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        parameters: AlphaFoldParameters,
        session: sqlalchemy.orm.session.Session,
        **kwargs,
    ):
        protein_id = parameters.protein_id
        self.log.debug(f"AlphaFold trigger called for protein_id={protein_id}")

        query = (
            session.query(Protein, Proposal).join(
                Proposal, Proposal.proposalId == Protein.proposalId
            )
        ).filter(Protein.proteinId == protein_id)
        protein, proposal = query.first()

        if proposal.proposalCode not in {"mx", "cm", "nt", "au"}:
            self.log.debug(
                f"Not triggering AlphaFold for protein_id={protein_id} with proposal_code={proposal.proposalCode}"
            )
            return {"success": True}

        if not protein.sequence:
            self.log.warning(
                f"AlphaFold triggered for Protein without a sequence (protein_id={protein_id})"
            )
            return False

        message = {
            "recipes": ["alphafold"],
            "parameters": {
                "ispyb_protein_id": protein_id,
                "ispyb_protein_sequence": protein.sequence,
                "ispyb_protein_name": protein.name,
            },
        }
        rw.transport.send("processing_recipe", message)
        self.log.info(f"AlphaFold triggered with parameters:\n{message}")
        # Because we don't return a jobid we have to manually record this metric
        self._metrics.record_metric("zocalo_trigger_jobs_total", ["alphafold"])

        return {"success": True}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_shelxt(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        parameters: ShelxtParameters,
        session: sqlalchemy.orm.session.Session,
        **kwargs,
    ):
        """Trigger a shelxt job for a given data collection."""

        dcid = parameters.dcid

        shelx_parameters: dict[str, list[Any]] = {
            "ins_file_location": [os.fspath(parameters.ins_file_location)],
            "prefix": [parameters.prefix],
            "scaling_id": [parameters.scaling_id],
        }

        self.log.debug("Shelxt trigger: Starting")

        jp = self.ispyb.mx_processing.get_job_params()
        jp["datacollectionid"] = dcid
        jp["display_name"] = "shelxt"
        jp["recipe"] = "postprocessing-shelxt"
        jp["automatic"] = parameters.automatic
        jp["comments"] = parameters.comment
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"Shelxt trigger: generated JobID {jobid}")

        for key, values in shelx_parameters.items():
            for value in values:
                jpp = self.ispyb.mx_processing.get_job_parameter_params()
                jpp["job_id"] = jobid
                jpp["parameter_key"] = key
                jpp["parameter_value"] = value
                jppid = self.ispyb.mx_processing.upsert_job_parameter(
                    list(jpp.values())
                )
                self.log.debug(f"Shelxt trigger: generated JobParameterID {jppid}")

        self.log.debug(f"Shelxt trigger: Processing job {jobid} created")

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info(f"Shelxt trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_ligand_fit(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        parameters: LigandFitParameters,
        session: sqlalchemy.orm.session.Session,
        **kwargs,
    ):
        """Trigger a ligand fit job for a given data collection.

        Trigger uses the 'final.pdb' and 'final.mtz' files which are output from
        DIMPLE, and requires a user submitted ligand SMILES code as inputs to
        the ligand fit pipeline

        Recipe parameters are described below with appropriate ispyb placeholder "{}"
        values:
        - target: set this to "ligand_fit"
        - dcid: the dataCollectionId for the given data collection i.e. "{ispyb_dcid}"
        - pdb: the output pdb from dimple i.e. "{ispyb_results_directory}/dimple/final.pdb"
        - mtz: the output mtz from dimple i.e. "{ispyb_results_directory}/dimple/final.mtz"
        - smiles: ligand SMILES code i.e. "{ispyb_smiles}"
        - pipeline: the pipeline to be used i.e. "phenix_pipeline"
        - comment: a comment to be stored in the ProcessingJob.comment field
        - scaling_id: scaling id of the data reduction pipeline that triggered dimple
          given as a list as this is how it is presented in the dimple recipe.
        - min_cc_keep: (optional) minimum correlation coefficient for ligand fitting to be considered successful
        - timeout-minutes: (optional) the max time (in minutes) allowed to wait for
          processing jobs to finish before skipping
        - automatic: boolean value passed to ProcessingJob.automatic field

        Example recipe parameters:
        { "target": "ligand_fit",
            "dcid": 123456,
            "pdb": "/path/to/pdb",
            "mtz": "/path/to/mtz",
            "smiles": "CN(CCC(N)=O)C[C@H]1O[C@H]([C@H](O)[C@@H]1O)n1c(C)nc2c(N)ncnc12",
            "beamline: i02-2,
            "pipeline": "phenix_pipeline",
            "automatic": true,
            "comment": "Ligand_fit triggered by xia2 multiplex",
            "scaling_id": [123456],
            "min_cc_keep": 0.7,
            "timeout-minutes": 115

        }
        """
        if parameters.smiles == "None":
            self.log.info(
                f"Skipping ligand fit trigger: DCID {parameters.dcid} has no associated SMILES string"
            )
            return {"success": True}

        protein_info = get_protein_for_dcid(parameters.dcid, session)

        proposal_id = getattr(protein_info, "proposalId", None)
        acronym = getattr(protein_info, "acronym", "Protein")

        query = (session.query(Proposal)).filter(Proposal.proposalId == proposal_id)
        proposal = query.first()

        if proposal_id:
            if proposal.proposalCode not in {"mx", "cm", "nt"}:
                self.log.debug(
                    f"Not triggering ligand fit pipeline for dcid {parameters.dcid} with proposal_code={proposal.proposalCode} due to Phenix licensing."
                )
                return {"success": True}

        if len(parameters.scaling_id) != 1:
            self.log.info(
                f"Skipping ligand fit trigger: exactly one scaling id must be provided, {len(parameters.scaling_id)} were given"
            )
            return {"success": True}

        scaling_id = parameters.scaling_id[0]

        # Get data collection information
        query = (
            (
                session.query(AutoProcProgram)
                .join(
                    AutoProc,
                    AutoProcProgram.autoProcProgramId == AutoProc.autoProcProgramId,
                )
                .join(
                    AutoProcScaling,
                    AutoProc.autoProcId == AutoProcScaling.autoProcId,
                )
            )
            .filter(AutoProcScaling.autoProcScalingId == scaling_id)
            .one()
        )

        allowed_upstream_programs = ["xia2 dials", "xia2.multiplex"]

        if parameters.beamline != "i02-2":
            if query.processingPrograms not in allowed_upstream_programs:
                self.log.info(
                    f"Skipping ligand_fit trigger: Upstream processing program is not in {allowed_upstream_programs} for beamline {parameters.beamline}."
                )
                return {"success": True}
        elif parameters.beamline == "i02-2":
            if query.processingPrograms != "xia2.multiplex":
                self.log.info(
                    f"Skipping ligand_fit trigger: Upstream processing program is not xia2.multiplex for beamline {parameters.beamline}."
                )
                return {"success": True}

        self.log.debug("Ligand_fit trigger: Starting")

        ligand_fit_parameters = {
            "dcid": parameters.dcid,
            "pdb": str(parameters.pdb),
            "mtz": str(parameters.mtz),
            "smiles": parameters.smiles,
            "pipeline": parameters.pipeline,
            "acronym": acronym,
        }

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = parameters.automatic
        jp["comments"] = parameters.comment
        jp["datacollectionid"] = parameters.dcid
        jp["display_name"] = "ligandfit"
        jp["recipe"] = "postprocessing-ligandfit"
        self.log.info(jp)
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"ligandfit trigger: generated JobID {jobid}")

        for key, value in ligand_fit_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(
                f"Ligand_fit trigger: generated JobParameterID {jppid} with {key}={value}"
            )

        self.log.debug(f"Ligand_fit_id trigger: Processing job {jobid} created")

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info(f"Ligand_fit_id trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_align_crystal(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        parameters: AlignCrystalParameters,
        session: sqlalchemy.orm.session.Session,
        **kwargs,
    ):
        if parameters.experiment_type != "Characterization":
            self.log.info(
                f"Skipping align_crystal trigger: experiment type {parameters.experiment_type} not supported"
            )
            return {"success": True}

        downstream_pipeline = {"fast_dp": "xoalign", "xia2-dials": "align-crystal"}

        jp = self.ispyb.mx_processing.get_job_params()
        jp["comments"] = parameters.comment
        jp["datacollectionid"] = parameters.dcid
        jp["display_name"] = downstream_pipeline[parameters.upstream_pipeline]
        jp["recipe"] = (
            f"postprocessing-{downstream_pipeline[parameters.upstream_pipeline]}"
        )
        self.log.info(jp)
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"align_crystal trigger: generated JobID {jobid}")

        align_crystal_parameters = {
            "experiment_file": parameters.experiment_file.as_posix(),
            "symlink": parameters.symlink,
        }

        for key, value in align_crystal_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(
                f"Align_crystal trigger: generated JobParameterID {jppid} with {key}={value}"
            )

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info(f"Align_crystal trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}
