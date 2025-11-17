from __future__ import annotations

import json
import pathlib
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
    pdb: pathlib.Path
    mtz: pathlib.Path
    prerun_threshold: int = pydantic.Field(default=300)
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    scaling_id: list[int]
    processing_directory: Optional[str] = None
    timeout: float = pydantic.Field(default=60, alias="timeout-minutes")
    backoff_delay: float = pydantic.Field(default=30, alias="backoff-delay")
    backoff_max_try: int = pydantic.Field(default=10, alias="backoff-max-try")
    backoff_multiplier: float = pydantic.Field(default=1.4, alias="backoff-multiplier")


class PanDDA_RhofitParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    datasets: list[str]
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    scaling_id: list[int]
    processing_directory: Optional[str] = None


class DLSTrigger_XChem(CommonService):
    """A service that creates and runs downstream processing jobs."""

    # Human readable service name
    _service_name = "DLS Trigger_XChem"

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
        if not hasattr(self, "trigger_" + target):  # trigger_xchem_ ?
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
        - prerun_threshold: the minimum number of comparator datasets needed to begin PanDDA
        - comment: a comment to be stored in the ProcessingJob.comment field
        - timeout-minutes: (optional) the max time (in minutes) allowed to wait for
        processing PanDDA jobs
        - automatic: boolean value passed to ProcessingJob.automatic field
        Example recipe parameters:
        { "target": "pandda_xchem",
            "dcid": 123456,
            "pdb": "/path/to/pdb",
            "mtz": "/path/to/mtz",
            "prerun_threshold": 300,
            "program_id": 123456,
            "scaling_id": [123456],
            "automatic": true,
            "comment": "PanDDA2 triggered by dimple",
            "timeout-minutes": 60,
        }
        """

        dcid = parameters.dcid
        scaling_id = parameters.scaling_id[0]
        # program_id = parameters.program_id
        _, ispyb_info = dlstbx.ispybtbx.ispyb_filter({}, {"ispyb_dcid": dcid}, session)
        visit = ispyb_info.get("ispyb_visit", "")
        visit_proposal = visit.split("-")[0]
        # visit_dir = pathlib.Path(ispyb_info.get("ispyb_visit_directory", ""))
        # results_dir = pathlib.Path(ispyb_info.get("ispyb_results_directory", ""))

        protein_info = get_protein_for_dcid(parameters.dcid, session)
        # protein_id = getattr(protein_info, "proteinId")
        proposal_id = getattr(protein_info, "proposalId")
        acronym = getattr(protein_info, "acronym")

        query = (session.query(Proposal)).filter(Proposal.proposalId == proposal_id)
        proposal = query.first()

        # 0. Check that this is an XChem expt, find .sqlite database
        if proposal.proposalCode not in {"lb"}:
            self.log.debug(
                f"Not triggering PanDDA2 pipeline for dcid={dcid} with proposal_code={proposal.proposalCode}"
            )
            return {"success": True}

        self.log.debug(
            f"proposal code is {proposal.proposalCode}, proposal number {proposal.proposalNumber}"
        )

        # TEMPORARY, OPENBIND TEST VISIT
        if proposal.proposalNumber not in {"42888"}:
            self.log.debug(
                f"Not triggering PanDDA2 pipeline for dcid={dcid}, only accepting data collections from lb42888 during test phase"
            )
            return {"success": True}

        # Find corresponding xchem visit directory and database
        xchem_dir = pathlib.Path(f"/dls/labxchem/data/{visit_proposal}")
        yaml_files = []

        for subdir in xchem_dir.iterdir():
            user_yaml = subdir / ".user.yaml"
            if user_yaml.exists():
                yaml_files.append(user_yaml)

        if not yaml_files:
            match_dir = False

        for yaml_file in yaml_files:
            with open(yaml_file, "r") as file:
                expt_yaml = yaml.load(file, Loader=yaml.SafeLoader)

            acr = expt_yaml["data"]["acronym"]
            directory = yaml_file.parents[0]
            if acr == acronym:
                match_dir = directory
                match_yaml = expt_yaml
                break
            else:
                match_dir = False

        if not match_dir:
            # Try reading from SoakDB .sqlite
            for subdir in xchem_dir.iterdir():
                if (subdir / ".user.yaml").exists():
                    continue
                try:
                    db_path = str(
                        subdir / "processing/database" / "soakDBDataFile.sqlite"
                    )
                    con = sqlite3.connect(db_path)
                    cur = con.cursor()
                    cur.execute("SELECT Protein FROM soakDB")
                    name = cur.fetchone()[0]
                    con.close()

                    if name is not None:
                        # visit = dir.parts[-1]
                        expt_yaml = {}
                        expt_yaml["data"] = {"acronym": name}
                        expt_yaml["autoprocessing"] = {}
                        expt_yaml["autoprocessing"]["pandda"] = {
                            "prerun-threshold": 300,
                            "heuristic": "default",
                        }

                        with open(subdir / ".user.yaml", "w") as f:
                            yaml.dump(expt_yaml, f)

                    if name == acronym:
                        match_dir = subdir
                        match_yaml = expt_yaml

                except Exception:
                    print(f"Unable to read .sqlite database for {subdir}")

        xchem_visit_dir = match_dir
        user_settings = match_yaml["autoprocessing"]

        if xchem_visit_dir:
            self.log.debug(
                f"Found a corresponding .sqlite database in XChem visit {xchem_visit_dir} for target {acronym}"
            )
        else:
            self.log.debug(
                f"Could not find a corresponding .sqlite database in XChem visit {xchem_dir} for target {acronym}, can't continue"
            )
            return {"success": True}

        processing_dir = xchem_visit_dir / "processing"
        sqlite_db = xchem_visit_dir / "processing/database" / "soakDBDataFile.sqlite"

        # Make a copy of the database for testing
        sqlite_db_copy = (
            xchem_visit_dir / "processing/database" / "auto_soakDBDataFile.sqlite"
        )
        if not sqlite_db_copy.exists():
            shutil.copy(str(sqlite_db), str(sqlite_db_copy))
            self.log.info(f"Made a copy of {sqlite_db}, auto_soakDBDataFile.sqlite")

        # Load any user specified processing parameters from user .yaml
        yaml_file = processing_dir / ".user.yaml"
        if yaml_file.exists():
            with open(yaml_file, "r") as file:
                user_yaml = yaml.safe_load(file)
        else:
            self.log.info(
                f"No user yaml found in processing directory {xchem_visit_dir / 'processing'}, proceeding with default settings."
            )

        # 1. Trigger when all upstream pipelines & related dimple jobs have finished

        program_list = [
            "xia2 dials",
            "xia2 3dii",
            "autoPROC",
            "autoPROC+STARANISO",
            "xia2.multiplex",
        ]  # will consider dimple output from these jobs to take forward

        # If other dimple/PanDDA2 job is running, quit, dimple set to trigger even if it fails
        min_start_time = datetime.now() - timedelta(hours=12)

        query = (
            (
                session.query(AutoProcProgram, ProcessingJob.dataCollectionId).join(
                    ProcessingJob,
                    ProcessingJob.processingJobId == AutoProcProgram.processingJobId,
                )
            )
            .filter(ProcessingJob.dataCollectionId == dcid)
            .filter(ProcessingJob.automatic == True)
            .filter(AutoProcProgram.processingPrograms.in_(["dimple", "PanDDA2"]))
            .filter(AutoProcProgram.recordTimeStamp > min_start_time)
            .filter(
                or_(
                    AutoProcProgram.processingStatus == None,
                    AutoProcProgram.processingStartTime == None,
                )
            )
        )

        if triggered_processing_job := query.first():
            self.log.info(
                f"Aborting PanDDA2 trigger as another dimple/PanDDA2 job has started for dcid {triggered_processing_job.dataCollectionId}"
            )
            return {"success": True}

        # Now check if other upstream pipeline is running and if so, checkpoint
        query = (
            (
                session.query(AutoProcProgram, ProcessingJob.dataCollectionId).join(
                    ProcessingJob,
                    ProcessingJob.processingJobId == AutoProcProgram.processingJobId,
                )
            )
            .filter(ProcessingJob.dataCollectionId == dcid)
            .filter(ProcessingJob.automatic == True)
            .filter(AutoProcProgram.recordTimeStamp > min_start_time)
            .filter(AutoProcProgram.processingPrograms.in_(program_list))
            .filter(
                or_(
                    AutoProcProgram.processingStatus == None,
                    AutoProcProgram.processingStartTime == None,
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
                    f"max-try exceeded, giving up waiting for related processings for appids {waiting_appids}\n"
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

        # Select the 'best' dataset to take forward based on criteria
        # default is I/sigI*completeness*#unique reflections,
        # others: highest resolution, lowest Rfree, dials only etc
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
            .filter(ProcessingJob.automatic == True)
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
            .filter(ProcessingJob.automatic == True)
            .filter(AutoProcProgram.processingPrograms == "dimple")
            .filter(AutoProcProgram.processingStatus == 1)  #!= None
            .filter(ProcessingJobParameter.parameterKey == "scaling_id")
            .filter(ProcessingJobParameter.parameterValue.in_(scaling_ids))  #
            .filter(AutoProcProgramAttachment.fileName == "final.pdb")
        )

        df2 = pd.read_sql(query.statement, query.session.bind)
        if df2.empty:
            self.log.info(
                f"No successful dimple jobs for dcid {dcid}, can't continue..."
            )
            return {"success": True}

        n_success_upstream = len(df)
        n_success_dimple = len(df2)  #

        self.log.info(
            f"There are {n_success_upstream} successful upstream jobs (excl fast-dp) & {n_success_dimple} successful dimple jobs, \
            selecting the best one based on heuristic: I/sigI*completeness * #unique reflections"
        )

        df2["parameterValue"] = pd.to_numeric(df2["parameterValue"]).astype("Int64")
        df3 = pd.merge(
            df2, df, left_on="parameterValue", right_on="autoProcScalingId", how="inner"
        ).sort_values("heuristic", ascending=False)

        if df3.empty:
            self.log.info("Problem finding 'best' dataset to take forward")
            return {"success": True}

        chosen_dataset_path = df3["filePath"][0]
        scaling_id = int(df3["autoProcScalingId"][0])
        pdb = chosen_dataset_path + "/final.pdb"
        mtz = chosen_dataset_path + "/final.mtz"

        self.log.debug("PanDDA2 trigger: Starting")

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

        # Read SQLite
        try:
            conn = sqlite3.connect(sqlite_db_copy)
            conn.execute("PRAGMA journal_mode=WAL;")
            df = pd.read_sql_query(
                f"SELECT * from mainTable WHERE Puck = '{code}' AND PuckPosition = {location} AND CrystalName = '{dtag}'",
                conn,
            )
            conn.close()
        except Exception as e:
            self.log.info(
                f"Could not read database for ligand information for dtag {dtag}: {e}"
            )
            return {"success": True}

        if len(df) != 1:
            self.log.info(
                f"Unique row in .sqlite for dcid {dcid}, puck {code}, puck position {location} cannot be found in database {sqlite_db_copy}, can't continue."
            )
            return {"success": True}

        # ProteinName = df["ProteinName"].item()
        LibraryName = df["LibraryName"].item()
        CompoundSMILES = df["CompoundSMILES"].item()
        CompoundCode = df["CompoundCode"].item()

        if LibraryName == "DMSO":  # DMSO solvent screen, not for PanDDA analysis
            self.log.info(
                f"Puck {code}, puck position {location} is from DMSO solvent screen, excluding from PanDDA analysis"
            )
            return {"success": True}
        elif not CompoundSMILES:
            self.log.info(
                f"Puck {code}, puck position {location} has no corresponding CompoundSMILES, considering as an apo dataset"
            )

        # 3. Create the dataset directory

        model_dir = processing_dir / "analysis" / "auto_model_building"
        dataset_dir = model_dir / dtag
        compound_dir = dataset_dir / "compound"
        self.log.info(f"Creating directory {dataset_dir}")
        pathlib.Path(compound_dir).mkdir(parents=True, exist_ok=True)  # False?
        dataset_list = [p.parts[-1] for p in model_dir.iterdir() if p.is_dir()]
        self.log.info(f"dataset_list is: {dataset_list}")
        dataset_count = sum(1 for p in model_dir.iterdir() if p.is_dir())

        # Copy the dimple files of the selected dataset
        shutil.copy(pdb, str(dataset_dir / "dimple.pdb"))
        shutil.copy(mtz, str(dataset_dir / "dimple.mtz"))

        with open(compound_dir / f"{CompoundCode}.smiles", "w") as smi_file:
            smi_file.write(CompoundSMILES)

        # 4. Job launch logic

        prerun_threshold = parameters.prerun_threshold

        if dataset_count < prerun_threshold:
            self.log.info(
                f"Dataset dataset_count {dataset_count} < PanDDA2 comparator dataset threshold of {prerun_threshold}, skipping for now..."
            )
            return {"success": True}
        elif dataset_count == prerun_threshold:
            datasets = dataset_list  # list of datasets to process
            self.log.info(
                f"Dataset dataset_count {dataset_count} = prerun_threshold of {prerun_threshold} datasets, launching PanDDA2 array job"
            )
        elif dataset_count > prerun_threshold:
            datasets = [dtag]
            self.log.info(f"Launching single PanDDA2 job for dtag {dtag}")

        self.log.debug("PanDDA2 trigger: Starting")
        self.log.info(f"datasets: {datasets}")

        self.log.debug(f"launching job for datasets: {datasets}")
        pandda_parameters = {
            "dcid": dcid,  #
            "processing_directory": str(processing_dir),
            "model_directory": str(model_dir),
            "dataset_directory": str(dataset_dir),
            "datasets": json.dumps(datasets),
            "n_datasets": len(datasets),
            "scaling_id": scaling_id,
            "prerun_threshold": prerun_threshold,
            "database_path": str(sqlite_db_copy),
        }

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = parameters.automatic
        # jp["comments"] = parameters.comment
        jp["datacollectionid"] = dcid
        jp["display_name"] = "PanDDA2"
        jp["recipe"] = "postprocessing-pandda2"
        self.log.info(jp)
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"PanDDA2 trigger: generated JobID {jobid}")

        for key, value in pandda_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(
                f"PanDDA2 trigger: generated JobParameterID {jppid} with {key}={value}"
            )

        self.log.debug(f"PanDDA2_id trigger: Processing job {jobid} created")

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info(f"PanDDA2_id trigger: Processing job {jobid} triggered")

        return {"success": True}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_pandda_xchem_post(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        message: Dict,
        parameters: PanDDAParameters,
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
            "automatic": true,
            "comment": "PanDDA2 post-run",
            "timeout-minutes": 60,
        }
        """

        dcid = parameters.dcid
        scaling_id = parameters.scaling_id[0]
        processing_directory = pathlib.Path(parameters.processing_directory)
        model_directory = processing_directory / "analysis" / "auto_model_building"

        _, ispyb_info = dlstbx.ispybtbx.ispyb_filter({}, {"ispyb_dcid": dcid}, session)
        visit = ispyb_info.get("ispyb_visit", "")
        visit_proposal = visit.split("-")[0]
        visit_number = visit.split("-")[1]

        # If other PanDDA2 postrun within visit running, quit
        min_start_time = datetime.now() - timedelta(hours=6)

        # from proposal and visit get all dcids
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
            .filter(ProcessingJob.automatic == True)
            .filter(AutoProcProgram.processingPrograms == "PanDDA2_post")
            .filter(AutoProcProgram.recordTimeStamp > min_start_time)
            .filter(
                or_(
                    AutoProcProgram.processingStatus == None,
                    AutoProcProgram.processingStartTime == None,
                )
            )
        )

        if triggered_processing_job := query.first():
            self.log.info(
                f"Aborting PanDDA2_postrun trigger as another postrun job has started for dcid {triggered_processing_job.dataCollectionId}"
            )
            return {"success": True}

        self.log.debug("PanDDA2 postrun trigger: Starting")

        processed_dir = (
            processing_directory / "analysis/auto_pandda2/processed_datasets"
        )
        datasets = []

        for subdir in processed_dir.iterdir():
            events_yaml = subdir / "events.yaml"
            if events_yaml.exists():
                datasets.append(subdir.parts[-1])

        pandda_parameters = {
            "dcid": dcid,  #
            "processing_directory": str(processing_directory),
            "model_directory": str(model_directory),
            "datasets": json.dumps(datasets),
            "scaling_id": scaling_id,
        }

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = parameters.automatic
        # jp["comments"] = parameters.comment
        jp["datacollectionid"] = dcid
        jp["display_name"] = "PanDDA2_post"
        jp["recipe"] = "postprocessing-pandda2-post"
        self.log.info(jp)
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"PanDDA2 postrun trigger: generated JobID {jobid}")

        for key, value in pandda_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(
                f"PanDDA2 trigger: generated JobParameterID {jppid} with {key}={value}"
            )

        self.log.debug(f"PanDDA2_id trigger: Processing job {jobid} created")

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info(f"PanDDA2_id trigger: Processing job {jobid} triggered")

        return {"success": True}

    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def trigger_pandda_rhofit(
        self,
        rw: workflows.recipe.RecipeWrapper,
        *,
        message: Dict,
        parameters: PanDDA_RhofitParameters,
        session: sqlalchemy.orm.session.Session,
        transaction: int,
        **kwargs,
    ):
        """Trigger a PanDDA rhofit job for an XChem fragment screening experiment.
        Recipe parameters are described below with appropriate ispyb placeholder "{}"
        values:
        - target: set this to "pandda_xchem_post"
        - dcid: the dataCollectionId for the given data collection i.e. "{ispyb_dcid}"
        - comment: a comment to be stored in the ProcessingJob.comment field
        - timeout-minutes: (optional) the max time (in minutes) allowed to wait for
        processing PanDDA jobs
        - automatic: boolean value passed to ProcessingJob.automatic field
        Example recipe parameters:
        { "target": "pandda_rhofit",
            "dcid": 123456,
            "datasets": ['dtag1','dtag2']
            "scaling_id": [123456],
            "automatic": true,
            "comment": "PanDDA2 Rhofit",
            "timeout-minutes": 60,
        }
        """

        dcid = parameters.dcid
        scaling_id = parameters.scaling_id[0]
        processing_directory = pathlib.Path(parameters.processing_directory)
        datasets = json.loads(parameters.get("datasets"))

        self.log.debug("PanDDA2 rhofit trigger: Starting")
        self.log.info(f"datasets: {datasets}")

        self.log.debug(f"launching job for datasets: {datasets}")
        pandda_parameters = {
            "dcid": dcid,  #
            "processing_directory": str(processing_directory),
            "datasets": json.dumps(datasets),
            "n_datasets": len(datasets),
            "scaling_id": scaling_id,
        }

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = parameters.automatic
        # jp["comments"] = parameters.comment
        jp["datacollectionid"] = dcid
        jp["display_name"] = "PanDDA2_Rhofit"
        jp["recipe"] = "postprocessing-pandda2-rhofit"
        self.log.info(jp)
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"PanDDA2 trigger: generated JobID {jobid}")

        for key, value in pandda_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(
                f"PanDDA2 trigger: generated JobParameterID {jppid} with {key}={value}"
            )

        self.log.debug(f"PanDDA2_Rhofit trigger: Processing job {jobid} created")

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info(f"PanDDA2_Rhofit trigger: Processing job {jobid} triggered")

        return {"success": True}
