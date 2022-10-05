from __future__ import annotations

import hashlib
import os
import pathlib
from datetime import datetime
from typing import Any, Dict, List, Literal, Mapping, Optional

import ispyb
import prometheus_client
import pydantic
import sqlalchemy.engine
import sqlalchemy.orm
import workflows.recipe
from ispyb.sqlalchemy import (
    PDB,
    AutoProcIntegration,
    AutoProcProgram,
    AutoProcProgramAttachment,
    BLSample,
    BLSession,
    Crystal,
    DataCollection,
    ProcessingJob,
    Proposal,
    Protein,
    ProteinHasPDB,
)
from sqlalchemy import or_
from sqlalchemy.orm import Load, contains_eager, joinedload
from workflows.recipe.wrapper import RecipeWrapper
from workflows.services.common_service import CommonService

from dlstbx.util import ChainMapWithReplacement
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
    scaling_id: int = pydantic.Field(gt=0)
    mtz: pathlib.Path
    pdb: list[PDBFileOrCode]
    automatic: Optional[bool] = False
    comment: Optional[str] = None


class ProteinInfo(pydantic.BaseModel):
    sequence: Optional[str] = None


class MrBumpParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    scaling_id: int = pydantic.Field(gt=0)
    protein_info: Optional[ProteinInfo] = None
    hklin: pathlib.Path
    pdb: list[PDBFileOrCode]
    automatic: Optional[bool] = False
    comment: Optional[str] = None


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
    diffraction_plan_info: Optional[DiffractionPlanInfo] = None
    program_id: int = pydantic.Field(gt=0)
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    spacegroup: Optional[str]


class BigEPLauncherParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    pipeline: Literal["autoSHARP", "AutoBuild", "Crank2"]
    data: pathlib.Path
    shelxc_path: pathlib.Path
    fast_ep_path: pathlib.Path
    program_id: int = pydantic.Field(gt=0)
    path_ext: Optional[str] = pydantic.Field(
        default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    automatic: Optional[bool] = False
    comment: Optional[str] = None


class FastEPParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    diffraction_plan_info: Optional[DiffractionPlanInfo] = None
    scaling_id: int = pydantic.Field(gt=0)
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    mtz: pathlib.Path


class BestParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    program_id: int = pydantic.Field(gt=0)
    data: pathlib.Path
    automatic: Optional[bool] = False
    comment: Optional[str] = None


class RelatedDCIDs(pydantic.BaseModel):
    dcids: List[int]
    sample_id: Optional[int] = pydantic.Field(gt=0)
    sample_group_id: Optional[int] = pydantic.Field(gt=0)


class MultiplexParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    related_dcids: List[RelatedDCIDs]
    wavelength: Optional[float] = pydantic.Field(gt=0)
    spacegroup: Optional[str]
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    backoff_delay: float = pydantic.Field(default=8, alias="backoff-delay")
    backoff_max_try: int = pydantic.Field(default=10, alias="backoff-max-try")
    backoff_multiplier: float = pydantic.Field(default=2, alias="backoff-multiplier")
    wavelength_tolerance: float = pydantic.Field(default=1e-4, ge=0)
    diffraction_plan_info: Optional[DiffractionPlanInfo] = None


class AlphaFoldParameters(pydantic.BaseModel):
    protein_id: int = pydantic.Field(gt=0)


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
        if target in {"big_ep_cluster", "big_ep_cloud"}:
            target = "big_ep_common"
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
                    message=message,
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

    def get_linked_pdb_files_for_dcid(
        self,
        session: sqlalchemy.orm.session.Session,
        dcid: int,
        pdb_tmpdir: pathlib.Path,
        user_pdb_dir: Optional[pathlib.Path] = None,
        ignore_pdb_codes: bool = False,
    ) -> List[PDBFileOrCode]:
        """Get linked PDB files for a given data collection ID.

        Valid PDB codes will be returned as the code, PDB files will be copied into a
        unique subdirectory within the `pdb_tmpdir` directory. Optionally search for
        PDB files in the `user_pdb_dir` directory.
        """
        pdb_files = []
        query = (
            session.query(DataCollection, PDB)
            .join(BLSample, BLSample.blSampleId == DataCollection.BLSAMPLEID)
            .join(Crystal, Crystal.crystalId == BLSample.crystalId)
            .join(Protein, Protein.proteinId == Crystal.proteinId)
            .join(ProteinHasPDB, ProteinHasPDB.proteinid == Protein.proteinId)
            .join(PDB, PDB.pdbId == ProteinHasPDB.pdbid)
            .filter(DataCollection.dataCollectionId == dcid)
        )
        for dc, pdb in query.all():
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
                    PDBFileOrCode(filepath=pdb_filepath, source=pdb.source)
                )

        if user_pdb_dir and user_pdb_dir.is_dir():
            # Look for matching .pdb files in user directory
            for f in user_pdb_dir.iterdir():
                if not f.stem or f.suffix != ".pdb" or not f.is_file():
                    continue
                self.log.info(f)
                pdb_files.append(PDBFileOrCode(filepath=f))
        return pdb_files

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
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
        dimple_parameters: dict[str, list[Any]] = {
            "data": [os.fspath(parameters.mtz)],
            "scaling_id": [parameters.scaling_id],
            "pdb": pdb_files,
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

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
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
        if proposal.proposalCode in ("lb", "in", "sw"):
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

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
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
        if proposal.proposalCode in ("lb", "in", "sw"):
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

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
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

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
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

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def trigger_fast_ep(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        parameters: FastEPParameters,
        session: sqlalchemy.orm.session.Session,
        **kwargs,
    ):
        if (
            not parameters.diffraction_plan_info
            or not parameters.diffraction_plan_info.anomalousScatterer
        ):
            self.log.info("Skipping fast_ep trigger: no anomalous scatterer specified")
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
        jp["recipe"] = "postprocessing-fast-ep"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"fast_ep trigger: generated JobID {jobid}")

        fast_ep_parameters = {
            "data": os.fspath(parameters.mtz),
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

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
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

        if not (parameters.protein_info and parameters.protein_info.sequence):
            self.log.info("Skipping mrbump trigger: sequence information not available")
            return {"success": True}

        jobids = []

        for pdb_files in {(), tuple(parameters.pdb)}:
            jp = self.ispyb.mx_processing.get_job_params()
            jp["automatic"] = parameters.automatic
            jp["comments"] = parameters.comment
            jp["datacollectionid"] = dcid
            jp["display_name"] = "MrBUMP"
            jp["recipe"] = "postprocessing-mrbump"
            jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
            jobids.append(jobid)
            self.log.debug(f"mrbump trigger: generated JobID {jobid}")

            mrbump_parameters = {
                "hklin": os.fspath(parameters.hklin),
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
                assert pdb_file.filepath is not None
                filepath = pdb_file.filepath
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

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def trigger_big_ep_common(
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
        if proposal.proposalCode in ("lb", "in", "sw"):
            self.log.info(f"Skipping big_ep trigger for {proposal.proposalCode} visit")
            return {"success": True}

        params = rw.recipe_step.get("parameters", {})
        target = params.get("target")

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = parameters.automatic
        jp["comments"] = parameters.comment
        jp["datacollectionid"] = parameters.dcid
        jp["display_name"] = parameters.pipeline
        if target == "big_ep_cluster":
            jp["recipe"] = "postprocessing-big-ep-cluster"
        elif target == "big_ep_cloud":
            jp["recipe"] = "postprocessing-big-ep-cloud"
        else:
            self.log.error(
                f"big_ep_common trigger failed: Invalid target specified {target}"
            )
            return False
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"big_ep_common trigger: generated JobID {jobid}")

        try:
            program_id = parameters.program_id
        except (TypeError, ValueError):
            self.log.error("big_ep_common trigger failed: Invalid program_id specified")
            return False
        big_ep_parameters = {
            "pipeline": parameters.pipeline,
            "program_id": program_id,
            "data": os.fspath(parameters.data),
            "path_ext": parameters.path_ext,
            "shelxc_path": os.fspath(parameters.shelxc_path),
            "fast_ep_path": os.fspath(parameters.fast_ep_path),
        }

        for key, value in big_ep_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(f"big_ep_cloud trigger: generated JobParameterID {jppid}")

        self.log.debug(f"big_ep_common trigger: Processing job {jobid} created")

        message = {
            "recipes": [],
            "parameters": {"ispyb_process": jobid},
        }
        rw.transport.send("processing_recipe", message)

        self.log.info(f"big_ep_common trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def trigger_big_ep(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        parameters: BigEPParameters,
        parameter_map: Mapping,
        session: sqlalchemy.orm.session.Session,
        **kwargs,
    ):
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
        if proposal.proposalCode in ("lb", "in", "sw"):
            self.log.info(f"Skipping big_ep trigger for {proposal.proposalCode} visit")
            return {"success": True}

        query = (
            session.query(AutoProcProgram)
            .join(
                AutoProcIntegration,
                AutoProcIntegration.autoProcProgramId
                == AutoProcProgram.autoProcProgramId,
            )
            .join(
                DataCollection,
                DataCollection.dataCollectionId == AutoProcIntegration.dataCollectionId,
            )
            .filter(DataCollection.dataCollectionId == dcid)
            .filter(AutoProcProgram.autoProcProgramId == parameters.program_id)
        )

        app = query.first()
        if not app:
            self.log.error(
                f"big_ep trigger failed: appid = {parameters.program_id} not found for dcid = {dcid}"
            )
            return False
        if blsession.beamLineName == "i23" and "multi" not in app.processingPrograms:
            self.log.info(
                f"Skipping big_ep trigger for {app.processingPrograms} data on i23"
            )
            return {"success": True}

        class BigEPParams(pydantic.BaseModel):
            data: pathlib.Path
            scaled_unmerged_mtz: pathlib.Path
            path_ext: str = pydantic.Field(
                default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S")
            )

        try:
            big_ep_params = BigEPParams(**parameter_map.get(app.processingPrograms, {}))
        except pydantic.ValidationError as e:
            self.log.error("big_ep trigger called with invalid parameters: %s", e)
            return False

        for inp_file in (big_ep_params.data, big_ep_params.scaled_unmerged_mtz):
            if not inp_file.is_file():
                self.log.info(
                    f"Skipping big_ep trigger: input file {inp_file} not found."
                )
                return {"success": True}

        path_ext = big_ep_params.path_ext
        spacegroup = parameters.spacegroup
        if spacegroup:
            path_ext += "-" + spacegroup

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = parameters.automatic
        jp["comments"] = parameters.comment
        jp["datacollectionid"] = dcid
        jp["display_name"] = "big_ep"
        jp["recipe"] = "postprocessing-big-ep"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"big_ep trigger: generated JobID {jobid}")

        big_ep_parameters = {
            "program_id": parameters.program_id,
            "data": os.fspath(big_ep_params.data),
            "scaled_unmerged_mtz": os.fspath(big_ep_params.scaled_unmerged_mtz),
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
                "program_id": parameters.program_id,
                "data": os.fspath(big_ep_params.data),
                "scaled_unmerged_mtz": os.fspath(big_ep_params.scaled_unmerged_mtz),
                "path_ext": path_ext,
                "force": False,
            },
            "recipes": [],
        }
        rw.transport.send("processing_recipe", message)

        self.log.info("big_ep triggered")

        return {"success": True, "return_value": None}

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
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
                    Load(DataCollection)
                    .load_only("dataCollectionId", "wavelength")
                    .raiseload("*"),
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
                if len(attachments) == 2:
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

            self.log.debug(set(dcids))
            self.log.debug(multiplex_job_dcids)
            if set(dcids) in multiplex_job_dcids:
                continue
            multiplex_job_dcids.append(set(dcids))

            jp = self.ispyb.mx_processing.get_job_params()
            jp["automatic"] = parameters.automatic
            jp["comments"] = parameters.comment
            jp["datacollectionid"] = dcid
            jp["display_name"] = "xia2.multiplex"
            jp["recipe"] = "postprocessing-xia2-multiplex"
            self.log.info(jp)
            jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
            jobids.append(jobid)
            self.log.debug(f"xia2.multiplex trigger: generated JobID {jobid}")

            query = (
                session.query(DataCollection)
                .filter(DataCollection.dataCollectionId.in_(dcids))
                .options(
                    Load(DataCollection)
                    .load_only(
                        "dataCollectionId",
                        "wavelength",
                        "startImageNumber",
                        "numberOfImages",
                    )
                    .raiseload("*")
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

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
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

        if proposal.proposalCode not in {"mx", "cm", "nt"}:
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
