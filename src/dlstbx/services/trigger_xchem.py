from __future__ import annotations

import ast
import json
import pathlib
import re
import shutil
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Optional

import ispyb
import pandas as pd
import prometheus_client
import pydantic
import sqlalchemy.engine
import sqlalchemy.orm
import workflows.recipe
import yaml
from ispyb.sqlalchemy import (
    AutoProc,
    AutoProcProgram,
    AutoProcProgramAttachment,
    AutoProcScaling,
    AutoProcScalingStatistics,
    BLSample,
    BLSession,
    Container,
    DataCollection,
    ProcessingJob,
    ProcessingJobParameter,
    Proposal,
)
from sqlalchemy import or_
from workflows.services.common_service import CommonService

import dlstbx.ispybtbx
from dlstbx.crud import get_protein_for_dcid
from dlstbx.util import ChainMapWithReplacement
from dlstbx.util.prometheus_metrics import BasePrometheusMetrics, NoMetrics


class PrometheusMetrics(BasePrometheusMetrics):
    def create_metrics(self):
        self.zocalo_trigger_jobs_total = prometheus_client.Counter(
            name="zocalo_trigger_jobs_total",
            documentation="The total number of jobs triggered by the Zocalo trigger service",
            labelnames=["target"],
        )


class PanDDAParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    comparator_threshold: int = pydantic.Field(default=300)
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    scaling_id: list[int]
    timeout: float = pydantic.Field(default=60, alias="timeout-minutes")
    backoff_delay: float = pydantic.Field(default=45, alias="backoff-delay")
    backoff_max_try: int = pydantic.Field(default=30, alias="backoff-max-try")
    backoff_multiplier: float = pydantic.Field(default=1.1, alias="backoff-multiplier")
    pipedream: Optional[bool] = True


class PanDDA_PostParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    scaling_id: list[int]
    processed_directory: str
    timeout: float = pydantic.Field(default=60, alias="timeout-minutes")


class DLSTriggerXChem(CommonService):
    """A service that creates and runs downstream processing jobs."""

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
    def trigger_pandda_xchem(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        message: Dict,
        parameters: PanDDAParameters,
        session: sqlalchemy.orm.session.Session,
        transaction: int,
        **kwargs,
    ):
        """Trigger a PanDDA job for an XChem fragment screening experiment.
        Trigger uses the 'final.pdb' and 'final.mtz' files which are output from the
        upstream DIMPLE job
        Recipe parameters are described below with appropriate ispyb placeholder "{}"
        values:
        - target: set this to "pandda_xchem"
        - dcid: the dataCollectionId for the given data collection i.e. "{ispyb_dcid}"
        - pdb: the output pdb from dimple i.e. "{ispyb_results_directory}/dimple/final.pdb"
        - mtz: the output mtz from dimple i.e. "{ispyb_results_directory}/dimple/final.mtz"
        - comparator_threshold: the minimum number of comparator datasets needed to begin PanDDA
        - comment: a comment to be stored in the ProcessingJob.comment field
        - timeout-minutes: (optional) the max time (in minutes) allowed to wait for
        processing PanDDA jobs
        - automatic: boolean value passed to ProcessingJob.automatic field
        Example recipe parameters:
        { "target": "pandda_xchem",
            "dcid": 123456,
            "comparator_threshold": 300,
            "scaling_id": [123456],
            "automatic": true,
            "comment": "PanDDA2 triggered by dimple",
            "timeout-minutes": 60,
        }
        """

        dcid = parameters.dcid
        scaling_id = parameters.scaling_id[0]
        comparator_threshold = parameters.comparator_threshold
        pipedream = parameters.pipedream

        protein_info = get_protein_for_dcid(parameters.dcid, session)
        # protein_id = getattr(protein_info, "proteinId")
        proposal_id = getattr(protein_info, "proposalId")
        acronym = getattr(protein_info, "acronym")

        query = (session.query(Proposal)).filter(Proposal.proposalId == proposal_id)
        proposal = query.first()

        # 0. Check that this is an XChem expt & locate .SQLite database
        if proposal.proposalCode not in {"lb"}:
            self.log.debug(
                f"Not triggering PanDDA2 pipeline for dcid={dcid} with proposal_code={proposal.proposalCode}"
            )
            return {"success": True}

        # TEMPORARY, FILTER BY OPENBIND VISIT
        if proposal.proposalNumber not in {"42888"}:
            self.log.debug(
                f"Not triggering PanDDA2 pipeline for dcid={dcid}, only accepting data collections from lb42888 during test phase"
            )
            return {"success": True}

        # Find corresponding XChem visit directory and database
        xchem_dir = pathlib.Path(
            f"/dls/labxchem/data/{proposal.proposalCode}{proposal.proposalNumber}"
        )
        yaml_files = []

        for subdir in xchem_dir.iterdir():
            user_yaml = subdir / ".user.yaml"
            if user_yaml.exists():
                yaml_files.append(user_yaml)

        if not yaml_files:
            match = False

        for yaml_file in yaml_files:
            with open(yaml_file, "r") as file:
                expt_yaml = yaml.load(file, Loader=yaml.SafeLoader)

            acr = expt_yaml["data"]["acronym"]
            directory = yaml_file.parents[0]
            if acr == acronym:
                match = True
                match_dir = directory
                # match_yaml = expt_yaml
                self.log.info(f"Found user yaml for dcid {dcid} at {yaml_file}")
                break
            else:
                match = False

        if not match:
            self.log.info(
                f"No user yaml found in {xchem_dir}, proceeding with default settings..."
            )
            # Try reading from SoakDB .sqlite
            for subdir in xchem_dir.iterdir():
                if (subdir / ".user.yaml").exists():
                    continue
                try:
                    db_path = str(
                        subdir / "processing/database" / "soakDBDataFile.sqlite"
                    )
                    con = sqlite3.connect(
                        f"file:{db_path}?mode=ro", uri=True, timeout=10
                    )
                    cur = con.cursor()
                    cur.execute("SELECT Protein FROM soakDB")
                    name = cur.fetchone()[0]
                    con.close()

                    if name is not None:
                        # visit = dir.parts[-1]
                        expt_yaml = {}
                        expt_yaml["data"] = {"acronym": name}
                        with open(subdir / ".user.yaml", "w") as f:
                            yaml.dump(expt_yaml, f)

                    if name == acronym:
                        match_dir = subdir
                        match = True
                        # match_yaml = expt_yaml

                except Exception as e:
                    print(f"Problem reading .sqlite database for {subdir}: {e}")

        if not match:
            self.log.debug(
                f"Exiting PanDDA2/Pipedream trigger: No directory found for {acronym}."
            )
            return {"success": True}
        else:
            xchem_visit_dir = match_dir
            # user_settings = match_yaml["autoprocessing"]

        if xchem_visit_dir:
            self.log.debug(
                f"Found a corresponding .sqlite database in XChem visit {xchem_visit_dir} for target {acronym}."
            )
        else:
            self.log.debug(
                f"Exiting PanDDA2/Pipedream trigger: can't find .sqlite database in XChem visit {xchem_dir} for target {acronym}."
            )
            return {"success": True}

        processing_dir = xchem_visit_dir / "processing"
        db = processing_dir / "database" / "soakDBDataFile.sqlite"
        processed_dir = xchem_visit_dir / "processed"

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

        # If other dimple/PanDDA2 job is running, quit, dimple set to trigger even if it fails?
        min_start_time = datetime.now() - timedelta(hours=6)

        query = (
            (
                session.query(AutoProcProgram, ProcessingJob.dataCollectionId).join(
                    ProcessingJob,
                    ProcessingJob.processingJobId == AutoProcProgram.processingJobId,
                )
            )
            .filter(ProcessingJob.dataCollectionId == dcid)
            .filter(ProcessingJob.automatic == True)  # noqa E711
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

        # Stop-gap, interval > max checkpoint time
        min_start_time = datetime.now() - timedelta(hours=3)

        query = (
            (
                session.query(AutoProcProgram, ProcessingJob.dataCollectionId).join(
                    ProcessingJob,
                    ProcessingJob.processingJobId == AutoProcProgram.processingJobId,
                )
            )
            .filter(ProcessingJob.dataCollectionId == dcid)
            .filter(ProcessingJob.automatic == True)  # noqa E711
            .filter(AutoProcProgram.processingPrograms.in_(["PanDDA2"]))
            .filter(AutoProcProgram.recordTimeStamp > min_start_time)
        )

        if triggered_processing_job := query.first():
            self.log.info(
                f"Exiting PanDDA2/Pipedream trigger: another PanDDA2 job was recently launched for dcid {dcid}"
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
            .filter(ProcessingJob.automatic == True)  # noqa E711
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
            .filter(ProcessingJob.automatic == True)  # noqa E711
            .filter(AutoProcProgram.processingPrograms.in_(program_list))
            .filter(AutoProcProgram.processingStatus == 1)
            .filter(AutoProcScalingStatistics.scalingStatisticsType == "overall")
        )

        df = pd.read_sql(query.statement, query.session.bind)
        df["heuristic"] = (
            df["meanIOverSigI"].astype(float)
            * df["completeness"].astype(float)
            * df["nTotalUniqueObservations"].astype(float)
        )
        # I/sigI*completeness*# unique reflections
        df = df[["autoProcScalingId", "heuristic"]].copy()
        scaling_ids = df["autoProcScalingId"].tolist()

        # find associated dimple jobs
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
            .filter(ProcessingJob.automatic == True)  # noqa E711
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

        n_success_upstream = len(df)
        n_success_dimple = len(df2)

        self.log.info(
            f"There are {n_success_upstream} successful upstream jobs (excl fast-dp) & {n_success_dimple} successful dimple jobs, \
            selecting the best one based on heuristic: I/sigI*completeness * #unique reflections"
        )

        df2["parameterValue"] = pd.to_numeric(df2["parameterValue"]).astype("Int64")
        df3 = pd.merge(
            df2, df, left_on="parameterValue", right_on="autoProcScalingId", how="inner"
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

        # 2. Get ligand information, location & container code

        query = (
            session.query(DataCollection, BLSample)
            .join(BLSample, BLSample.blSampleId == DataCollection.BLSAMPLEID)
            .join(Container, Container.containerId == BLSample.containerId)
            .filter(DataCollection.dataCollectionId == dcid)
        )

        query = query.with_entities(BLSample.location, BLSample.name, Container.code)
        location = int(query.one()[0])  # never multiple?
        dtag = query.one()[1]
        code = query.one()[2]

        # Read XChem SQLite database for ligand info
        try:
            conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=10)
            df = pd.read_sql_query(
                f"SELECT * from mainTable WHERE Puck = '{code}' AND PuckPosition = {location} AND CrystalName = '{dtag}'",
                conn,
            )

        except Exception as e:
            self.log.info(
                f"Exiting PanDDA2/Pipedream trigger: Exception whilst reading ligand information from {db} for dtag {dtag}, dcid {dcid}: {e}"
            )
            return {"success": True}

        finally:
            if "conn" in locals():
                conn.close()

        if len(df) != 1:
            self.log.info(
                f"Exiting PanDDA2/Pipedream trigger: Unique row in .sqlite for dtag {dtag}, puck {code}, puck position {location} cannot be found in {db}, skipping dcid {dcid}"
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

        # 3. Create the dataset directory
        analysis_dir = processed_dir / "analysis"
        pandda_dir = analysis_dir / "pandda2"
        model_dir = pandda_dir / "model_building"
        dataset_dir = model_dir / dtag
        compound_dir = dataset_dir / "compound"
        self.log.info(f"Creating directory {dataset_dir}")
        pathlib.Path(compound_dir).mkdir(parents=True, exist_ok=True)
        dataset_list = sorted([p.parts[-1] for p in model_dir.iterdir() if p.is_dir()])
        dataset_count = sum(1 for p in model_dir.iterdir() if p.is_dir())
        self.log.info(f"Dataset count is: {dataset_count}")

        # Copy the dimple files of the selected dataset
        shutil.copy(pdb, str(dataset_dir / "dimple.pdb"))
        shutil.copy(mtz, str(dataset_dir / "dimple.mtz"))
        shutil.copy(upstream_mtz, str(dataset_dir / f"{dtag}.free.mtz"))

        with open(compound_dir / f"{CompoundCode}.smiles", "w") as smi_file:
            smi_file.write(CompoundSMILES)

        # Check if Pipedream job was recently launched
        min_start_time = datetime.now() - timedelta(hours=3)

        query = (
            (
                session.query(AutoProcProgram, ProcessingJob.dataCollectionId).join(
                    ProcessingJob,
                    ProcessingJob.processingJobId == AutoProcProgram.processingJobId,
                )
            )
            .filter(ProcessingJob.dataCollectionId == dcid)
            .filter(ProcessingJob.automatic == True)  # noqa E711
            .filter(AutoProcProgram.processingPrograms.in_(["Pipedream"]))
            .filter(AutoProcProgram.recordTimeStamp > min_start_time)
        )

        if triggered_processing_job := query.first():
            self.log.info(
                f"Pipedream job recently launched for dcid {dcid}, skipping pipedream trigger"
            )
            pipedream = False

        # Create seperate pipedream directory
        if pipedream:
            pipedream_dir = analysis_dir / "pipedream"
            model_dir_pd = pipedream_dir / "model_building"
            dataset_dir_pd = model_dir_pd / dtag
            compound_dir_pd = dataset_dir_pd / "compound"
            self.log.info(f"Creating directory {dataset_dir_pd}")
            pathlib.Path(compound_dir_pd).mkdir(parents=True, exist_ok=True)
            shutil.copy(pdb, str(dataset_dir_pd / "dimple.pdb"))
            shutil.copy(mtz, str(dataset_dir_pd / "dimple.mtz"))
            shutil.copy(upstream_mtz, str(dataset_dir_pd / f"{dtag}.free.mtz"))

            with open(compound_dir_pd / f"{CompoundCode}.smiles", "w") as smi_file:
                smi_file.write(CompoundSMILES)

        # 4. Job launch logic

        recipe_parameters = {
            "dcid": dcid,
            "processed_directory": str(processed_dir),
            "model_directory": str(model_dir),
            "dtag": dtag,
            "n_datasets": 1,
            "scaling_id": scaling_id,
            "comparator_threshold": comparator_threshold,
            "database_path": str(db),
            "upstream_mtz": pathlib.Path(upstream_mtz).parts[-1],
            "smiles": str(CompoundSMILES),
        }

        if dataset_count < comparator_threshold:
            self.log.info(
                f"{dataset_count} < comparator dataset threshold of {comparator_threshold}, skipping PanDDA2 for now..."
            )

            if pipedream:
                self.log.info(f"Launching Pipedream for dtag {dtag}")
                self.upsert_proc(rw, dcid, "Pipedream", recipe_parameters)
            return {"success": True}

        elif dataset_count == comparator_threshold:
            recipe_parameters["n_datasets"] = len(dataset_list)

            with open(model_dir / ".batch.json", "w") as f:
                json.dump(dataset_list, f)

            self.log.info(
                f"{dataset_count} = comparator dataset threshold of {comparator_threshold}, launching PanDDA2 array job"
            )
            self.upsert_proc(rw, dcid, "PanDDA2", recipe_parameters)

            if pipedream:
                self.log.info(f"Launching Pipedream for dtag {dtag}")
                self.upsert_proc(rw, dcid, "Pipedream", recipe_parameters)

        elif dataset_count > comparator_threshold:
            self.log.info(f"Launching single PanDDA2 job for dtag {dtag}")
            self.upsert_proc(rw, dcid, "PanDDA2", recipe_parameters)

            if pipedream:
                self.log.info(f"Launching Pipedream for dtag {dtag}")
                self.upsert_proc(rw, dcid, "Pipedream", recipe_parameters)

        return {"success": True}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_pandda_xchem_post(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        message: Dict,
        parameters: PanDDA_PostParameters,
        session: sqlalchemy.orm.session.Session,
        transaction: int,
        **kwargs,
    ):
        """Trigger a PanDDA post-run job for an XChem fragment screening experiment.
        Recipe parameters are described below with appropriate ispyb placeholder "{}"
        values:
        - target: set this to "pandda_xchem_post"
        - dcid: the dataCollectionId for the given data collection i.e. "{ispyb_dcid}"
        - comment: a comment to be stored in the ProcessingJob.comment field
        - timeout-minutes: (optional) the max time (in minutes) allowed to wait for
        processing PanDDA jobs
        - automatic: boolean value passed to ProcessingJob.automatic field
        Example recipe parameters:
        { "target": "pandda_xchem_post",
            "dcid": 123456,
            "scaling_id": [123456],
            "processing_directory": '/dls/labxchem/data/lb42888/lb42888-1/processing',
            "automatic": true,
            "comment": "PanDDA2 post-run",
        }
        """

        dcid = parameters.dcid
        scaling_id = parameters.scaling_id[0]
        processed_directory = pathlib.Path(parameters.processed_directory)

        _, ispyb_info = dlstbx.ispybtbx.ispyb_filter({}, {"ispyb_dcid": dcid}, session)
        visit = ispyb_info.get("ispyb_visit", "")
        visit_proposal = visit.split("-")[0]
        visit_number = visit.split("-")[1]

        # From proposal and visit get all dcids
        query = (
            session.query(Proposal, BLSession, DataCollection)
            .join(BLSession, BLSession.proposalId == Proposal.proposalId)
            .join(DataCollection, DataCollection.SESSIONID == BLSession.sessionId)
            .filter(Proposal.proposalCode == visit_proposal[0:2])
            .filter(Proposal.proposalNumber == visit_proposal[2::])
            .filter(BLSession.visit_number == visit_number)
        )

        df = pd.read_sql(query.statement, query.session.bind)
        dcids = df["dataCollectionId"].tolist()

        query = (
            (
                session.query(AutoProcProgram, ProcessingJob.dataCollectionId).join(
                    ProcessingJob,
                    ProcessingJob.processingJobId == AutoProcProgram.processingJobId,
                )
            )
            .filter(ProcessingJob.dataCollectionId.in_(dcids))
            .filter(ProcessingJob.automatic == True)  # noqa E711
            .filter(AutoProcProgram.processingPrograms == "PanDDA2-post")
            .filter(
                or_(
                    AutoProcProgram.processingStatus == None,  # noqa E711
                    AutoProcProgram.processingStartTime == None,  # noqa E711
                )
            )
        )

        if triggered_processing_job := query.first():
            self.log.info(
                f"Aborting PanDDA2_postrun trigger as another postrun job has started for dcid {triggered_processing_job.dataCollectionId}"
            )
            return {"success": True}

        self.log.debug("PanDDA2 postrun trigger: Starting")

        recipe_parameters = {
            "dcid": dcid,  #
            "processed_directory": str(processed_directory),
            "scaling_id": scaling_id,
        }

        self.upsert_proc(rw, dcid, "PanDDA2-post", recipe_parameters)

        return {"success": True}
