from __future__ import annotations

import ast
import json
import pathlib
import re
import shutil
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Optional

import gemmi
import ispyb
import pandas as pd
import prometheus_client
import pydantic
import sqlalchemy.engine
import sqlalchemy.orm
import workflows.recipe
from ispyb.sqlalchemy import (
    AutoProc,
    AutoProcProgram,
    AutoProcProgramAttachment,
    AutoProcScaling,
    AutoProcScalingStatistics,
    BLSample,
    Container,
    Crystal,
    DataCollection,
    ProcessingJob,
    ProcessingJobParameter,
    Proposal,
    Protein,
)
from sqlalchemy import or_
from workflows.services.common_service import CommonService

import dlstbx.ispybtbx
from dlstbx.crud import get_latest_dcid_for_dtag, get_protein_for_dcid
from dlstbx.util import ChainMapWithReplacement
from dlstbx.util.prometheus_metrics import BasePrometheusMetrics, NoMetrics
from dlstbx.util.soakdb import find_xchem_visit_dir


class PrometheusMetrics(BasePrometheusMetrics):
    def create_metrics(self):
        self.zocalo_trigger_jobs_total = prometheus_client.Counter(
            name="zocalo_trigger_jobs_total",
            documentation="The total number of jobs triggered by the Zocalo trigger service",
            labelnames=["target"],
        )


class ModelBuildingParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    comparator_threshold: int = pydantic.Field(default=350)
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    scaling_id: list[int]
    timeout: float = pydantic.Field(default=180, alias="timeout-minutes")
    backoff_delay: float = pydantic.Field(default=20, alias="backoff-delay")
    backoff_max_try: int = pydantic.Field(default=10, alias="backoff-max-try")
    backoff_multiplier: float = pydantic.Field(default=2, alias="backoff-multiplier")
    pipedream: Optional[bool] = True
    overwrite: Optional[bool] = False
    bulk_array: Optional[bool] = None


class HitIndentificationParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    xchem_visit_dir: str
    comparator_threshold: int = pydantic.Field(default=300)
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    scaling_id: list[int]
    timeout: float = pydantic.Field(default=180, alias="timeout-minutes")
    pipedream: Optional[bool] = True
    overwrite: Optional[bool] = False
    bulk_array: Optional[bool] = False


class CollateParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    program_id: int = pydantic.Field(gt=0)
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    scaling_id: list[int]
    processing_directory: str
    timeout: float = pydantic.Field(default=60, alias="timeout-minutes")
    backoff_delay: float = pydantic.Field(default=20, alias="backoff-delay")
    backoff_max_try: int = pydantic.Field(default=10, alias="backoff-max-try")
    backoff_multiplier: float = pydantic.Field(default=2, alias="backoff-multiplier")
    pipedream: Optional[bool] = False
    overwrite: Optional[bool] = False


class DLSTriggerXChem(CommonService):
    """A service that creates and runs downstream processing jobs for XChem."""

    # Human readable service name
    _service_name = "DLS TriggerXChem"

    # Logger name
    _logger_name = "dlstbx.services.trigger_xchem"

    def initializing(self):
        """Subscribe to the trigger_xchem queue. Received messages must be acknowledged."""
        self._ispyb_sessionmaker = sqlalchemy.orm.sessionmaker(
            bind=sqlalchemy.create_engine(
                ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
            )
        )
        workflows.recipe.wrap_subscribe(
            self._transport,
            "trigger_xchem",
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

    def upsert_proc(self, rw, dcid, procname, recipe_parameters):
        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = True
        # jp["comments"] = parameters.comment
        jp["datacollectionid"] = dcid
        jp["display_name"] = "procname"
        jp["recipe"] = f"postprocessing-{procname.lower()}"
        self.log.info(jp)
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"{procname} trigger: generated JobID {jobid}")

        for key, value in recipe_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(
                f"{procname} trigger: generated JobParameterID {jppid} with {key}={value}"
            )

        self.log.debug(f"{procname}_id trigger: Processing job {jobid} created")

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info(f"{procname}_id trigger: Processing job {jobid} triggered")

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_modelbuilding(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        message: Dict,
        parameters: ModelBuildingParameters,
        session: sqlalchemy.orm.session.Session,
        transaction: int,
        **kwargs,
    ):
        """Select a dimple model to take forward and stage a dataset for the
        downstream hit-identification pipelines.

        Waits (with exponential backoff) for related upstream pipelines and
        dimple jobs to finish, then selects the 'best' dataset by
        I/sigI * completeness * #unique-reflections, preferring those cases
        processed in the user-defined spacegroup and the most recent processing
        batch. Reads the soakDB for ligand info, skipping DMSO solvent screens
        and crystals with no CompoundSMILES.

        Copies the chosen dimple files into the shared model_building directory,
        writes the ligand .smiles file, and fires a single ligand-restraints job
        (grade2 default) per dcid. On success that recipe sends control to
        trigger_hitidentification.
        """

        dcid = parameters.dcid
        scaling_id = parameters.scaling_id[0]
        comparator_threshold = parameters.comparator_threshold
        pipedream = parameters.pipedream
        overwrite = parameters.overwrite
        bulk_array = parameters.bulk_array

        protein_info = get_protein_for_dcid(parameters.dcid, session)
        # protein_id = getattr(protein_info, "proteinId")
        proposal_id = getattr(protein_info, "proposalId")
        acronym = getattr(protein_info, "acronym")

        # TEMPORARY PROPOSAL FILTER
        ALLOWED_PROPOSALS = ["lb42888", "sw44043", "sw44107", "lb36049"]
        PROPOSAL_ALIASES = {"mx41448": "lb42888"}

        query = (session.query(Proposal)).filter(Proposal.proposalId == proposal_id)
        proposal = query.first()
        proposal_code = proposal.proposalCode
        proposal_number = proposal.proposalNumber
        data_proposal = proposal_code + proposal_number
        proposal_string = PROPOSAL_ALIASES.get(data_proposal, data_proposal)

        # 0. Check that this is an XChem expt & locate .SQLite database
        if proposal_string not in ALLOWED_PROPOSALS:
            self.log.debug(
                f"Not triggering PanDDA2 pipeline for dcid={dcid} proposal {proposal_string}"
            )
            return {"success": True}

        # Get sample details
        query = (
            session.query(DataCollection, BLSample)
            .join(BLSample, BLSample.blSampleId == DataCollection.BLSAMPLEID)
            .join(Container, Container.containerId == BLSample.containerId)
            .filter(DataCollection.dataCollectionId == dcid)
        )

        query = query.with_entities(BLSample.name, BLSample.location, Container.code)
        dtag = query.one()[0]
        location = int(query.one()[1])
        container_code = query.one()[2]

        # Check for crystal recollections
        latest_dcid = get_latest_dcid_for_dtag(dtag, session)
        if latest_dcid and latest_dcid != dcid:
            self.log.info(
                f"Exiting PanDDA2/Pipedream trigger: dcid {dcid} is not the latest for dtag {dtag}; Recollection underway?"
            )
            return {"success": True}

        # Get the user defined spacegroup
        query = (
            session.query(Crystal.spaceGroup)
            .join(BLSample, BLSample.crystalId == Crystal.crystalId)
            .filter(BLSample.name == dtag)
        )

        if query.first()[0]:
            user_sg = gemmi.find_spacegroup_by_name(query.first()[0]).hm

        # Find corresponding XChem visit directory and database
        xchem_dir = pathlib.Path(f"/dls/labxchem/data/{proposal_string}")
        xchem_visit_dir = find_xchem_visit_dir(
            xchem_dir, acronym, container_code, location, dtag, self.log
        )

        if xchem_visit_dir is None:
            self.log.debug(
                f"Exiting PanDDA2/Pipedream trigger: No labxchem directory found for {acronym}."
            )
            return {"success": True}

        processing_dir = xchem_visit_dir / "processing"
        self.log.debug(
            f"Found a corresponding .sqlite database in XChem visit {xchem_visit_dir} for target {acronym}."
        )

        # 1. Trigger when all upstream pipelines & related dimple jobs have finished
        program_list = [
            "xia2 dials",
            "xia2 3dii",
            "autoPROC",
            "autoPROC+STARANISO",
            "xia2.multiplex",
        ]  # will consider dimple output from these jobs to take forward

        query = (
            session.query(AutoProcProgram.processingPrograms)
            .join(
                AutoProc,
                AutoProcProgram.autoProcProgramId == AutoProc.autoProcProgramId,
            )
            .join(
                AutoProcScaling,
                AutoProc.autoProcId == AutoProcScaling.autoProcId,
            )
        ).filter(AutoProcScaling.autoProcScalingId == scaling_id)

        if query.first()[0] == "fast_dp":
            self.log.info(
                "Exiting PanDDA2/Pipedream trigger: upstream processingProgram is fast_dp"
            )
            return {"success": True}

        # If other dimple/PanDDA2 job is running, quit, dimple will trigger PanDDA2 even if it fails
        min_start_time = datetime.now() - timedelta(hours=6)

        query = (
            (
                session.query(AutoProcProgram, ProcessingJob.dataCollectionId).join(
                    ProcessingJob,
                    ProcessingJob.processingJobId == AutoProcProgram.processingJobId,
                )
            )
            .filter(ProcessingJob.dataCollectionId == dcid)
            .filter(AutoProcProgram.processingPrograms.in_(["dimple", "PanDDA2"]))
            .filter(AutoProcProgram.recordTimeStamp > min_start_time)
            .filter(
                or_(
                    AutoProcProgram.processingStatus == None,  # noqa E711
                    AutoProcProgram.processingStartTime == None,  # noqa E711
                )
            )
        )

        if triggered_processing_job := query.first():
            self.log.info(
                f"Exiting PanDDA2/Pipedream trigger: another {triggered_processing_job.AutoProcProgram.processingPrograms} job has started for dcid {triggered_processing_job.dataCollectionId}"
            )
            return {"success": True}

        # Now check if other upstream pipeline is running and if so, checkpoint (it might fail)
        min_start_time = datetime.now() - timedelta(hours=6)
        query = (
            (
                session.query(AutoProcProgram, ProcessingJob.dataCollectionId).join(
                    ProcessingJob,
                    ProcessingJob.processingJobId == AutoProcProgram.processingJobId,
                )
            )
            .filter(ProcessingJob.dataCollectionId == dcid)
            .filter(AutoProcProgram.recordTimeStamp > min_start_time)
            .filter(AutoProcProgram.processingPrograms.in_(program_list))
            .filter(
                or_(
                    AutoProcProgram.processingStatus == None,  # noqa E711
                    AutoProcProgram.processingStartTime == None,  # noqa E711
                )
            )
        )

        # Calculate message delay for exponential backoff in case an upstream
        # processing program  is still running, in which case we checkpoint
        # with the calculated message delay
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

        # If there are any running (or yet to start) jobs, then checkpoint with delay
        waiting_processing_jobs = query.all()
        if n_waiting_processing_jobs := len(waiting_processing_jobs):
            self.log.info(
                f"Waiting on {n_waiting_processing_jobs} processing jobs for {dcid=}"
            )
            waiting_appids = [
                row.AutoProcProgram.autoProcProgramId for row in waiting_processing_jobs
            ]
            if status["ntry"] >= parameters.backoff_max_try:
                # Give up waiting for this program to finish and trigger
                # pandda with remaining related results are available
                self.log.info(
                    f"Max-try exceeded, giving up waiting for related processings for appids {waiting_appids}\n"
                )
            else:
                # Send results to myself for next round of processing
                self.log.debug(f"Waiting for appids={waiting_appids}")
                rw.checkpoint(
                    {
                        "trigger-status": status,
                    },
                    delay=message_delay,
                    transaction=transaction,
                )

                return {"success": True}

        # Select the 'best' dataset to take forward based on criteria,
        # default is I/sigI*completeness*#unique reflections
        query = (
            (
                session.query(
                    AutoProcProgram,
                    AutoProcScalingStatistics,
                    AutoProcScaling.autoProcScalingId,
                    AutoProc.spaceGroup,
                )
                .join(
                    ProcessingJob,
                    ProcessingJob.processingJobId == AutoProcProgram.processingJobId,
                )
                .join(
                    AutoProc,
                    AutoProcProgram.autoProcProgramId == AutoProc.autoProcProgramId,
                )
                .join(
                    AutoProcScaling, AutoProc.autoProcId == AutoProcScaling.autoProcId
                )
                .join(
                    AutoProcScalingStatistics,
                    AutoProcScalingStatistics.autoProcScalingId
                    == AutoProcScaling.autoProcScalingId,
                )
            )
            .filter(ProcessingJob.dataCollectionId == dcid)
            .filter(AutoProcProgram.processingPrograms.in_(program_list))
            .filter(AutoProcProgram.processingStatus == 1)
            .filter(AutoProcScalingStatistics.scalingStatisticsType == "overall")
        )

        df = pd.read_sql(query.statement, query.session.bind)

        # prioritise datasets processed in user-defined spacegroup
        if "user_sg" in locals():
            df_filteredbysg = df[df["spaceGroup"] == user_sg]

            if not df_filteredbysg.empty:
                df = df_filteredbysg
                n_success_upstream = len(df)
                self.log.info(
                    f"There are {n_success_upstream} successful upstream jobs (excl fast-dp) in the user-defined spacegroup {user_sg} \
                    selecting the best one based on I/sigI*completeness * #unique reflections, from the most recent processing batch"
                )

        # rank datasets by I/sigI*completeness*# unique reflections
        df["heuristic"] = (
            df["meanIOverSigI"].astype(float)
            * df["completeness"].astype(float)
            * df["nTotalUniqueObservations"].astype(float)
        )

        df = df[["autoProcScalingId", "heuristic"]].copy()
        scaling_ids = df["autoProcScalingId"].tolist()

        # find associated dimple jobs from scaling_ids, take most recent batch if reprocessing
        query = (
            (
                session.query(
                    AutoProcProgram,
                    AutoProcProgramAttachment,
                    ProcessingJobParameter.parameterKey,
                    ProcessingJobParameter.parameterValue,
                )
                .join(
                    ProcessingJob,
                    ProcessingJob.processingJobId == AutoProcProgram.processingJobId,
                )
                .join(
                    ProcessingJobParameter,
                    ProcessingJobParameter.processingJobId
                    == ProcessingJob.processingJobId,
                )
                .join(
                    AutoProcProgramAttachment,
                    AutoProcProgramAttachment.autoProcProgramId
                    == AutoProcProgram.autoProcProgramId,
                )
            )
            .filter(ProcessingJob.dataCollectionId == dcid)
            .filter(AutoProcProgram.processingPrograms == "dimple")
            .filter(AutoProcProgram.processingStatus == 1)
            .filter(ProcessingJobParameter.parameterKey == "scaling_id")
            .filter(ProcessingJobParameter.parameterValue.in_(scaling_ids))
            .filter(AutoProcProgramAttachment.fileName == "final.pdb")
        )

        df2 = pd.read_sql(query.statement, query.session.bind)
        if df2.empty:
            self.log.info(
                f"Exiting PanDDA2/Pipedream trigger: No successful dimple jobs for dcid {dcid}, skipping..."
            )
            return {"success": True}

        # mark as new batch whenever the gap between jobs is >= 12 hours, consider most recent batch
        df2 = df2.sort_values("processingStartTime").reset_index(drop=True)
        df2["time_diff"] = df2["processingStartTime"].diff()
        df2["batch"] = (df2["time_diff"] >= pd.Timedelta(hours=12)).cumsum() + 1
        recent_batch = df2[df2["batch"] == df2["batch"].max()].copy()

        recent_batch["parameterValue"] = pd.to_numeric(
            recent_batch["parameterValue"]
        ).astype("Int64")
        df3 = pd.merge(
            recent_batch,
            df,
            left_on="parameterValue",
            right_on="autoProcScalingId",
            how="inner",
        ).sort_values("heuristic", ascending=False)

        if df3.empty:
            self.log.info(
                f"Exiting PanDDA2/Pipedream trigger: Issue selecting dataset to take forward for dcid {dcid}"
            )
            return {"success": True}

        chosen_dataset_path = df3["filePath"][0]
        self.log.debug(
            f"Chosen dataset to take forward: {chosen_dataset_path} for dcid {dcid}"
        )
        scaling_id = int(df3["autoProcScalingId"][0])
        environment = df3["processingEnvironment"][0]
        environment = re.search(r"data=(\[[^\]]*\])", environment)

        if environment:
            upstream_mtz = ast.literal_eval(environment.group(1))[0]
            self.log.info(f"Chosen mtz for dcid {dcid} is {upstream_mtz}")
        else:
            self.log.info(
                f"Exiting PanDDA2/Pipedream trigger: no environment information for dcid {dcid}"
            )
            return {"success": True}

        pdb = chosen_dataset_path + "/final.pdb"
        mtz = chosen_dataset_path + "/final.mtz"

        self.log.debug("PanDDA2/Pipedream trigger: Starting")

        # 2. Read XChem SQLite database for ligand info

        db_master = processing_dir / "database" / "soakDBDataFile.sqlite"

        try:
            conn = sqlite3.connect(f"file:{db_master}?mode=ro", uri=True, timeout=10)
            df = pd.read_sql_query(
                f"SELECT * from mainTable WHERE Puck = '{container_code}' AND PuckPosition = {location} AND CrystalName = '{dtag}'",
                conn,
            )

        except Exception as e:
            self.log.info(
                f"Exiting PanDDA2/Pipedream trigger: Exception whilst reading ligand information from {db_master} for dtag {dtag}, dcid {dcid}: {e}"
            )
            return {"success": True}

        finally:
            if "conn" in locals():
                conn.close()

        if len(df) != 1:
            self.log.info(
                f"Exiting PanDDA2/Pipedream trigger: Unique row in .sqlite for dtag {dtag}, puck {container_code}, puck position {location} cannot be found in {db_master}, skipping dcid {dcid}"
            )
            return {"success": True}

        LibraryName = df["LibraryName"].item()
        CompoundSMILES = df["CompoundSMILES"].item()
        CompoundCode = df["CompoundCode"].item()

        if LibraryName == "DMSO":
            self.log.info(
                f"Exiting PanDDA2/Pipedream trigger: {dtag} is DMSO solvent screen, skipping..."
            )
            return {"success": True}
        elif not CompoundSMILES or str(CompoundSMILES).strip().lower() in [
            "none",
            "null",
            "nan",
            "",
        ]:
            self.log.info(
                f"Exiting PanDDA2/Pipedream trigger: {dtag} has no corresponding CompoundSMILES, skipping..."
            )
            return {"success": True}

        # 3. Create dataset directory structure (single shared model_building dir)
        auto_dir = processing_dir / "auto"
        analysis_dir = auto_dir / "analysis"
        model_dir = analysis_dir / "model_building"
        dataset_dir = model_dir / dtag
        compound_dir = dataset_dir / "compound"

        self.log.info(f"Creating directory {dataset_dir}")

        if not overwrite:
            try:
                compound_dir.mkdir(parents=True, exist_ok=False)
            except FileExistsError:
                self.log.info(
                    f"Exiting model_building trigger: {dataset_dir} already exists"
                )
                return {"success": True}
        else:
            compound_dir.mkdir(parents=True, exist_ok=True)

        # Copy the dimple files of the selected dataset
        shutil.copy(pdb, str(dataset_dir / "dimple.pdb"))
        shutil.copy(mtz, str(dataset_dir / "dimple.mtz"))
        shutil.copy(upstream_mtz, str(dataset_dir / f"{dtag}.free.mtz"))

        with open(compound_dir / f"{CompoundCode}.smiles", "w") as smi_file:
            smi_file.write(CompoundSMILES)

        # 4. Fire a single ligand-restraints job; it will trigger hitidentification on success
        recipe_parameters = {
            "dcid": dcid,
            "xchem_visit_dir": str(xchem_visit_dir),
            "processing_directory": str(processing_dir),
            "model_directory": str(model_dir),
            "dtag": dtag,
            "scaling_id": scaling_id,
            "comparator_threshold": comparator_threshold,
            "database_path": str(db_master),
            "upstream_mtz": pathlib.Path(upstream_mtz).parts[-1],
            "pipedream": pipedream,
            "overwrite": overwrite,
        }

        self.log.info(f"Launching ligand-restraints for dtag {dtag} (dcid {dcid})")
        self.upsert_proc(rw, dcid, "Grade2", recipe_parameters)
        return {"success": True}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_hitidentification(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        message: Dict,
        parameters: HitIndentificationParameters,
        session: sqlalchemy.orm.session.Session,
        transaction: int,
        **kwargs,
    ):
        """Launch PanDDA2 / Pipedream once restraints for this dcid are ready.

        Records the current dcid and its dtag in model_dir/.batch_dcids.json as a
        {dcid: dtag} map, so dtags can be read back at threshold without
        re-querying ispyb. Pipedream fires for the current dcid on every call.
        PanDDA2 is gated by the count of recorded dcids vs. comparator_threshold:
        below threshold → skip; at threshold → fire one per-dcid PanDDA2 job for
        each recorded dcid; above threshold → single PanDDA2 for the current dtag.

        bulk_array=True: iterate model_dir directly, write the dataset list to
        .bulk_array.json, and fire one array job over dtags in model_building.
        """
        dcid = parameters.dcid
        scaling_id = parameters.scaling_id[0]
        comparator_threshold = parameters.comparator_threshold
        pipedream = parameters.pipedream
        overwrite = parameters.overwrite
        bulk_array = parameters.bulk_array

        # Re-derive paths from labxchem visit parameter
        xchem_visit_dir = pathlib.Path(parameters.xchem_visit_dir)
        processing_dir = xchem_visit_dir / "processing"
        model_dir = processing_dir / "auto" / "analysis" / "model_building"
        db_master = processing_dir / "database" / "soakDBDataFile.sqlite"

        # Resolve dtag for the current dcid
        query = (
            session.query(BLSample.name)
            .join(DataCollection, BLSample.blSampleId == DataCollection.BLSAMPLEID)
            .filter(DataCollection.dataCollectionId == dcid)
        )
        row = query.first()
        if not row:
            self.log.info(f"Exiting hitidentification trigger: no BLSample for {dcid}")
            return {"success": True}
        dtag = row[0]

        recipe_parameters = {
            "dcid": dcid,
            "xchem_visit_dir": str(xchem_visit_dir),
            "processing_directory": str(processing_dir),
            "model_directory": str(model_dir),
            "dtag": dtag,
            "n_datasets": 1,
            "scaling_id": scaling_id,
            "comparator_threshold": comparator_threshold,
            "database_path": str(db_master),
            "pipedream": pipedream,
            "overwrite": overwrite,
        }

        if bulk_array:
            dataset_list = sorted(
                [p.parts[-1] for p in model_dir.iterdir() if p.is_dir()]
            )
            dataset_count = len(dataset_list)
            recipe_parameters["n_datasets"] = dataset_count
            with open(model_dir / ".bulk_array.json", "w") as f:
                json.dump(dataset_list, f)
            self.log.info(
                f"bulk_array=True, launching PanDDA2 array job over {dataset_count} datasets"
            )
            self.upsert_proc(rw, dcid, "PanDDA2-array", recipe_parameters)
            if pipedream:
                self.log.info(f"Launching Pipedream for dtag {dtag}")
                self.upsert_proc(rw, dcid, "Pipedream-array", recipe_parameters)
            return {"success": True}

        # Record this dcid and its dtag in the hidden gating json, so the dtag
        # can be read back at threshold without re-querying ispyb. JSON keys are
        # strings, so dcids round-trip as str.
        dcids_file = model_dir / ".batch_dcids.json"
        if dcids_file.exists():
            with open(dcids_file, "r") as f:
                recorded_dcids = json.load(f)
        else:
            recorded_dcids = {}
        if str(dcid) not in recorded_dcids:
            recorded_dcids[str(dcid)] = dtag
            with open(dcids_file, "w") as f:
                json.dump(recorded_dcids, f)

        dataset_count = len(recorded_dcids)
        self.log.info(f"Recorded waiting dcid count is: {dataset_count}")

        if pipedream:
            self.log.info(f"Launching Pipedream for dtag {dtag}")
            self.upsert_proc(rw, dcid, "Pipedream", recipe_parameters)

        if dataset_count < comparator_threshold:
            self.log.info(
                f"{dataset_count} < comparator dataset threshold of {comparator_threshold}, skipping PanDDA2 for now..."
            )
            return {"success": True}

        if dataset_count == comparator_threshold:
            self.log.info(
                f"{dataset_count} = comparator dataset threshold of {comparator_threshold}, launching per-dcid PanDDA2 jobs"
            )
            for batch_dcid, batch_dtag in recorded_dcids.items():
                batch_dcid = int(batch_dcid)
                batch_params = {
                    **recipe_parameters,
                    "dcid": batch_dcid,
                    "dtag": batch_dtag,
                    "n_datasets": 1,
                }
                self.upsert_proc(rw, batch_dcid, "PanDDA2", batch_params)
            return {"success": True}

        # dataset_count > comparator_threshold
        self.log.info(f"Launching single PanDDA2 job for dtag {dtag}")
        self.upsert_proc(rw, dcid, "PanDDA2", recipe_parameters)
        return {"success": True}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_xchem_collate(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        message: Dict,
        parameters: CollateParameters,
        session: sqlalchemy.orm.session.Session,
        transaction: int,
        **kwargs,
    ):
        """Trigger an XChem Collate job once a target's collection run is complete.

        Gathers every dcid for this target (matched by Protein acronym) under the
        proposal of the triggering dcid's visit, then gates on those jobs:
        aborts if any PanDDA2/Pipedream/xia2 program newer than program_id has
        started (a fresh batch is underway), and checkpoints with exponential
        backoff while any of them are still running. Once processing has settled
        and no other XChemCollate job is already in flight for the target, fires
        a single XChemCollate job keyed on the highest dcid.

        Recipe parameters (ispyb placeholders shown as "{}"):
        - target: set this to "xchem_collate"
        - dcid: the dataCollectionId i.e. "{ispyb_dcid}"
        - program_id: the AutoProcProgramId of the triggering job
        - scaling_id: list of scaling ids i.e. ["{scaling_id}"]
        - processing_directory: the labxchem visit processing dir
        - pipedream / overwrite: forwarded to the collate wrapper
        - comment: stored in the ProcessingJob.comment field
        - automatic: boolean passed to ProcessingJob.automatic
        Example recipe parameters:
        { "target": "xchem_collate",
            "dcid": 123456,
            "program_id": 123456,
            "scaling_id": [123456],
            "processing_directory": '/dls/labxchem/data/lb42888/lb42888-1/processing',
            "automatic": true,
        }
        """

        dcid = parameters.dcid
        program_id = parameters.program_id
        scaling_id = parameters.scaling_id[0]
        processing_directory = pathlib.Path(parameters.processing_directory)
        overwrite = parameters.overwrite
        pipedream = parameters.pipedream

        _, ispyb_info = dlstbx.ispybtbx.ispyb_filter({}, {"ispyb_dcid": dcid}, session)
        visit = ispyb_info.get("ispyb_visit", "")
        visit_proposal = visit.split("-")[0]
        visit_number = visit.split("-")[1]

        protein_info = get_protein_for_dcid(parameters.dcid, session)
        acronym = getattr(protein_info, "acronym")

        # get the dcids for the protein target under the current proposal
        dcids = [
            row[0]
            for row in session.query(DataCollection.dataCollectionId)
            .join(BLSample, BLSample.blSampleId == DataCollection.BLSAMPLEID)
            .join(Crystal, Crystal.crystalId == BLSample.crystalId)
            .join(Protein, Protein.proteinId == Crystal.proteinId)
            .join(Proposal, Proposal.proposalId == Protein.proposalId)
            .join(
                ProcessingJob,
                ProcessingJob.dataCollectionId == DataCollection.dataCollectionId,
            )
            .join(
                AutoProcProgram,
                AutoProcProgram.processingJobId == ProcessingJob.processingJobId,
            )
            .filter(Proposal.proposalCode == visit_proposal[0:2])
            .filter(Proposal.proposalNumber == visit_proposal[2::])
            .filter(Protein.acronym == acronym)
            .distinct()
            .all()
        ]

        # trigger on the final PanDDA/Pipedream program_id from the current
        # processing batch
        query = (
            (
                session.query(AutoProcProgram, ProcessingJob.dataCollectionId).join(
                    ProcessingJob,
                    ProcessingJob.processingJobId == AutoProcProgram.processingJobId,
                )
            )
            .filter(ProcessingJob.dataCollectionId.in_(dcids))
            .filter(
                AutoProcProgram.processingPrograms.in_(
                    ["xia2 dials", "PanDDA2", "Pipedream"]
                )
            )
            .filter(AutoProcProgram.autoProcProgramId > program_id)  # noqa E711
        )

        if triggered_processing_job := query.first():
            self.log.info(
                f"Aborting xchem_collate trigger for dcid {dcid} as processing job has been started for dcid {triggered_processing_job.dataCollectionId}"
            )
            return {"success": True}

        # has processing finished? checkpoint if not
        min_start_time = datetime.now() - timedelta(hours=8)
        query = (
            (
                session.query(AutoProcProgram, ProcessingJob.dataCollectionId).join(
                    ProcessingJob,
                    ProcessingJob.processingJobId == AutoProcProgram.processingJobId,
                )
            )
            .filter(ProcessingJob.dataCollectionId.in_(dcids))
            .filter(
                AutoProcProgram.processingPrograms.in_(
                    ["xia2 dials", "PanDDA2", "Pipedream"]
                )
            )
            .filter(
                or_(
                    AutoProcProgram.processingStatus == None,  # noqa E711
                    AutoProcProgram.processingStartTime == None,  # noqa E711
                )
            )
            .filter(AutoProcProgram.recordTimeStamp > min_start_time)  # noqa E711
        )

        # Calculate message delay for backoff when max program id has finished but there remains
        # some jobs to finish in which case we checkpoint with the calculated message delay
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

        # If there are any running (or yet to start) jobs, then checkpoint with delay
        waiting_processing_jobs = query.all()
        if n_waiting_processing_jobs := len(waiting_processing_jobs):
            self.log.info(
                f"Waiting on {n_waiting_processing_jobs} processing jobs for {dcid=} for XChemCollate"
            )
            waiting_appids = [
                row.AutoProcProgram.autoProcProgramId for row in waiting_processing_jobs
            ]
            if status["ntry"] >= parameters.backoff_max_try:
                # Give up waiting for this program to finish and trigger
                # collate with remaining results that are available
                self.log.info(
                    f"Max-try exceeded, giving up waiting for related processings for appids {waiting_appids}\n"
                )
            else:
                # Send results to myself for next round of processing
                self.log.debug(f"Waiting for appids={waiting_appids}")
                rw.checkpoint(
                    {
                        "trigger-status": status,
                    },
                    delay=message_delay,
                    transaction=transaction,
                )

                return {"success": True}

        self.log.debug(
            f"PanDDA2/Pipedream processing has finished for {visit_proposal}-{visit_number}"
        )

        # Stop-gap
        min_start_time = datetime.now() - timedelta(minutes=180)

        query = (
            session.query(AutoProcProgram, ProcessingJob.dataCollectionId)
            .join(
                ProcessingJob,
                ProcessingJob.processingJobId == AutoProcProgram.processingJobId,
            )
            .filter(ProcessingJob.dataCollectionId.in_(dcids))
            .filter(AutoProcProgram.processingPrograms.in_(["XChemCollate"]))
            .filter(AutoProcProgram.recordTimeStamp > min_start_time)
            .filter(
                or_(
                    AutoProcProgram.processingStatus == None,  # noqa E711
                    AutoProcProgram.processingStartTime == None,  # noqa E711
                )
            )
        )

        if triggered_processing_job := query.first():
            self.log.info(
                f"Exiting XChemCollate trigger: another XChemCollate job has been launched for {acronym}"
            )
            return {"success": True}

        self.log.debug("XChemCollate trigger: Starting")

        recipe_parameters = {
            "dcid": max(dcids),
            "processing_directory": str(processing_directory),
            "scaling_id": scaling_id,
            "pipedream": pipedream,
            "overwrite": overwrite,
        }
        # Upsert on max dcid
        self.upsert_proc(rw, max(dcids), "XChem-Collate", recipe_parameters)

        return {"success": True}
