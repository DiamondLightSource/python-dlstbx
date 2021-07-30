import hashlib
import logging
import pathlib
from datetime import datetime

import ispyb
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
from sqlalchemy.orm import Load, contains_eager, joinedload
from workflows.services.common_service import CommonService


class DLSTrigger(CommonService):
    """A service that creates and runs downstream processing jobs."""

    # Human readable service name
    _service_name = "DLS Trigger"

    # Logger name
    _logger_name = "dlstbx.services.trigger"

    def initializing(self):
        """Subscribe to the trigger queue. Received messages must be acknowledged."""
        logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
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

        txn = rw.transport.transaction_begin()
        rw.set_default_channel("output")

        def parameters(parameter, replace_variables=True):
            if isinstance(message, dict):
                base_value = message.get(parameter, params.get(parameter))
            else:
                base_value = params.get(parameter)
            if (
                not replace_variables
                or not base_value
                or not isinstance(base_value, str)
                or "$" not in base_value
            ):
                return base_value
            for key in rw.environment:
                if "$" + key in base_value:
                    base_value = base_value.replace("$" + key, str(rw.environment[key]))
            return base_value

        with self._ispyb_sessionmaker() as session:
            result = getattr(self, "trigger_" + target)(
                rw=rw,
                header=header,
                message=message,
                parameters=parameters,
                session=session,
                transaction=txn,
            )
        if result and result.get("success"):
            rw.send({"result": result.get("return_value")}, transaction=txn)
            rw.transport.ack(header, transaction=txn)
        else:
            rw.transport.transaction_abort(txn)
            rw.transport.nack(header)
            return
        rw.transport.transaction_commit(txn)

    def trigger_dimple(self, rw, header, parameters, session, **kwargs):
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
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("Dimple trigger failed: No DCID specified")
            return False

        pdb_tmpdir = pathlib.Path(parameters("pdb_tmpdir"))

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
            if pdb.code is not None:
                pdb_code = pdb.code.strip()
                if pdb_code.isalnum() and len(pdb_code) == 4:
                    pdb_files.append(pdb_code)
                    continue
                elif pdb_code != "":
                    self.log.warning(
                        f"Invalid input PDB code for running Dimple: {pdb.code}"
                    )
            if pdb.contents not in ("", None):
                sha1 = hashlib.sha1(pdb.contents.encode()).hexdigest()
                assert pdb.name and "/" not in pdb.name, "Invalid PDB file name"
                pdb_dir = pdb_tmpdir / sha1
                pdb_dir.mkdir(parents=True, exist_ok=True)
                pdb_filepath = pdb_dir / pdb.name
                if not pdb_filepath.exists():
                    pdb_filepath.write_text(pdb.contents)
                pdb_files.append(str(pdb_filepath))

        if parameters("user_pdb_directory"):
            # Look for matching .pdb files in user directory
            user_pdb_dir = pathlib.Path(parameters("user_pdb_directory"))
            if user_pdb_dir.is_dir():
                for f in user_pdb_dir.iterdir():
                    if not f.stem or f.suffix != ".pdb" or not f.is_file():
                        continue
                    self.log.info(f)
                    pdb_files.append(str(f))

        if not pdb_files:
            self.log.info(
                "Skipping dimple trigger: DCID %s has no associated PDB information"
                % dcid
            )
            return {"success": True}
        self.log.info("PDB files: %s", ", ".join(pdb_files))

        dc = (
            session.query(DataCollection)
            .filter(DataCollection.dataCollectionId == dcid)
            .one()
        )
        dimple_parameters = {
            "data": [parameters("mtz")],
            "scaling_id": [parameters("scaling_id")],
            "pdb": pdb_files,
        }
        if parameters("set_synchweb_status"):
            dimple_parameters["set_synchweb_status"] = [1]

        jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
        jisp["datacollectionid"] = dcid
        jisp["start_image"] = dc.startImageNumber
        jisp["end_image"] = dc.startImageNumber + dc.numberOfImages - 1

        self.log.debug("Dimple trigger: Starting")

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = bool(parameters("automatic"))
        jp["comments"] = parameters("comment")
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

    def trigger_ep_predict(self, rw, header, parameters, session, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("ep_predict trigger failed: No DCID specified")
            return False

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

        diffraction_plan_info = parameters("diffraction_plan_info")
        if not diffraction_plan_info:
            self.log.info(
                "Skipping ep_predict trigger: diffraction plan information not available"
            )
            return {"success": True}
        try:
            anom_scatterer = diffraction_plan_info["anomalousScatterer"]
            if not anom_scatterer:
                self.log.info(
                    "Skipping ep_predict trigger: No anomalous scatterer specified"
                )
                return {"success": True}
        except Exception:
            self.log.info(
                "Skipping ep_predict trigger: Cannot read anomalous scatterer setting"
            )
            return {"success": True}
        try:
            program = parameters("program")
        except Exception:
            self.log.warning("ep_predict trigger: Upstream program name not specified")
            program = ""
        try:
            program_id = int(parameters("program_id"))
        except (TypeError, ValueError):
            self.log.error("ep_predict trigger failed: Invalid program_id specified")
            return False

        jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
        jisp["datacollectionid"] = dcid
        jisp["start_image"] = dc.startImageNumber
        jisp["end_image"] = dc.startImageNumber + dc.numberOfImages - 1

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = bool(parameters("automatic"))
        jp["comments"] = parameters("comment")
        jp["datacollectionid"] = dcid
        jp["display_name"] = "ep_predict"
        jp["recipe"] = "postprocessing-ep-predict"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"ep_predict trigger: generated JobID {jobid}")

        ep_parameters = {
            "program": program,
            "program_id": program_id,
            "data": parameters("data"),
            "threshold": parameters("threshold"),
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
                "program": program,
                "data": parameters("data"),
                "threshold": parameters("threshold"),
            },
            "recipes": [],
        }
        rw.transport.send("processing_recipe", message)

        self.log.info(f"ep_predict trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    def trigger_mr_predict(self, rw, header, parameters, session, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("mr_predict trigger failed: No DCID specified")
            return False

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

        diffraction_plan_info = parameters("diffraction_plan_info")
        if not diffraction_plan_info:
            self.log.info(
                "Skipping mr_predict trigger: diffraction plan information not available"
            )
            return {"success": True}
        try:
            program_id = int(parameters("program_id"))
        except (TypeError, ValueError):
            self.log.error("mr_predict trigger failed: Invalid program_id specified")
            return False
        try:
            program = parameters("program")
        except Exception:
            self.log.warning("mr_predict trigger: Upstream program name not specified")
            program = ""

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = bool(parameters("automatic"))
        jp["comments"] = parameters("comment")
        jp["datacollectionid"] = dcid
        jp["display_name"] = "mr_predict"
        jp["recipe"] = "postprocessing-mr-predict"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"mr_predict trigger: generated JobID {jobid}")

        mr_parameters = {
            "program_id": program_id,
            "program": program,
            "data": parameters("data"),
            "threshold": parameters("threshold"),
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
                "program": program,
                "data": parameters("data"),
                "threshold": parameters("threshold"),
            },
            "recipes": [],
        }
        rw.transport.send("processing_recipe", message)

        self.log.info(f"mr_predict trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    def trigger_screen19_mx(self, rw, header, parameters, session, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("screen19_mx trigger failed: No DCID specified")
            return False

        visit = parameters("visit")
        test_visit = parameters("test_visit")
        if visit and test_visit and visit != test_visit:
            self.log.info(
                f"screen19_mx trigger: processing is enabled only for testing in {test_visit}"
            )
            return {"success": True}

        try:
            program_id = int(parameters("program_id"))
        except (TypeError, ValueError):
            self.log.error("screen19_mx trigger failed: Invalid program_id specified")
            return False

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = bool(parameters("automatic"))
        jp["comments"] = parameters("comment")
        jp["datacollectionid"] = dcid
        jp["display_name"] = "screen19_mx"
        jp["recipe"] = "postprocessing-screen19-mx"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"screen19_mx trigger: generated JobID {jobid}")

        screen19_parameters = {
            "program_id": program_id,
            "data": parameters("data"),
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
                "data": parameters("data"),
            },
            "recipes": [],
        }
        rw.transport.send("processing_recipe", message)

        self.log.info(f"screen19_mx trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    def trigger_best(self, rw, header, parameters, session, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("best trigger failed: No DCID specified")
            return False

        diffraction_plan_info = parameters("diffraction_plan_info")
        if not diffraction_plan_info:
            self.log.info(
                "Skipping best trigger: diffraction plan information not available"
            )
            return {"success": True}
        try:
            program_id = int(parameters("program_id"))
        except (TypeError, ValueError):
            self.log.error("best trigger failed: Invalid program_id specified")
            return False

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = bool(parameters("automatic"))
        jp["comments"] = parameters("comment")
        jp["datacollectionid"] = dcid
        jp["display_name"] = "best"
        jp["recipe"] = "postprocessing-best"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug("best trigger: generated JobID {}".format(jobid))

        best_parameters = {"program_id": program_id, "data": parameters("data")}

        for key, value in best_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug("best trigger: generated JobParameterID {}".format(jppid))

        self.log.debug("best trigger: Processing job {} created".format(jobid))

        message = {
            "parameters": {"ispyb_process": jobid, "data": parameters("data")},
            "recipes": [],
        }
        rw.transport.send("processing_recipe", message)

        self.log.info("best trigger: Processing job {} triggered".format(jobid))

        return {"success": True, "return_value": jobid}

    def trigger_fast_ep(self, rw, header, parameters, session, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("fast_ep trigger failed: No DCID specified")
            return False

        diffraction_plan_info = parameters("diffraction_plan_info")
        if not diffraction_plan_info:
            self.log.info(
                "Skipping fast_ep trigger: diffraction plan information not available"
            )
            return {"success": True}
        try:
            anom_scatterer = diffraction_plan_info["anomalousScatterer"]
            if not anom_scatterer:
                self.log.info(
                    "Skipping fast_ep trigger: No anomalous scatterer specified"
                )
                return {"success": True}
        except Exception:
            self.log.info(
                "Skipping fast_ep trigger: Cannot read anomalous scatterer setting"
            )
            return {"success": True}

        query = session.query(DataCollection).filter(
            DataCollection.dataCollectionId == dcid
        )
        dc = query.one()
        jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
        jisp["datacollectionid"] = dcid
        jisp["start_image"] = dc.startImageNumber
        jisp["end_image"] = dc.startImageNumber + dc.numberOfImages - 1

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = bool(parameters("automatic"))
        jp["comments"] = parameters("comment")
        jp["datacollectionid"] = dcid
        jp["display_name"] = "fast_ep"
        jp["recipe"] = "postprocessing-fast-ep"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"fast_ep trigger: generated JobID {jobid}")

        fast_ep_parameters = {
            "check_go_fast_ep": bool(parameters("automatic")),
            "data": parameters("mtz"),
            "scaling_id": parameters("scaling_id"),
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

    def trigger_mrbump(self, rw, header, parameters, session, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("mrbump trigger failed: No DCID specified")
            return False

        protein_info = parameters("protein_info")
        try:
            if not protein_info["sequence"]:
                self.log.info(
                    "Skipping mrbump trigger: sequence information not available"
                )
                return {"success": True}
        except Exception:
            self.log.info("Skipping mrbump trigger: Cannot read sequence information")
            return {"success": True}

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = bool(parameters("automatic"))
        jp["comments"] = parameters("comment")
        jp["datacollectionid"] = dcid
        jp["display_name"] = "MrBUMP"
        jp["recipe"] = "postprocessing-mrbump"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"mrbump trigger: generated JobID {jobid}")

        mrbump_parameters = {
            "hklin": parameters("hklin"),
            "scaling_id": parameters("scaling_id"),
        }

        for key, value in mrbump_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(f"fast_ep trigger: generated JobParameterID {jppid}")

        self.log.debug(f"mrbump trigger: Processing job {jobid} created")

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info(f"mrbump trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    def trigger_big_ep_launcher(self, rw, header, parameters, session, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("big_ep_launcher trigger failed: No DCID specified")
            return False
        pipeline = parameters("pipeline")
        if not pipeline:
            self.log.error("big_ep_launcher trigger failed: No pipeline specified")
            return False

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = bool(parameters("automatic"))
        jp["comments"] = parameters("comment")
        jp["datacollectionid"] = dcid
        jp["display_name"] = pipeline
        jp["recipe"] = "postprocessing-big-ep-launcher"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"big_ep_launcher trigger: generated JobID {jobid}")

        try:
            program_id = int(parameters("program_id"))
        except (TypeError, ValueError):
            self.log.error(
                "big_ep_launcher trigger failed: Invalid program_id specified"
            )
            return False
        data = parameters("data")
        if not data:
            self.log.error(
                "big_ep_launcher trigger failed: No input data file specified"
            )
            return False
        path_ext = parameters("path_ext")
        if not path_ext:
            path_ext = datetime.now().strftime("%Y%m%d_%H%M%S")
        shelxc_path = parameters("shelxc_path")
        fast_ep_path = parameters("fast_ep_path")

        msg = rw.payload
        big_ep_parameters = {
            "pipeline": pipeline,
            "program_id": program_id,
            "data": data,
            "atom": msg.get("atom"),
            "dataset": "|".join([ds["name"] for ds in msg.get("datasets", [])]),
            "spacegroup": msg.get("spacegroup"),
            "nsites": msg.get("nsites"),
            "compound": "Protein",
            "sequence": msg.get("sequence"),
        }

        for key, value in big_ep_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(f"big_ep_laucher trigger: generated JobParameterID {jppid}")

        self.log.debug(f"big_ep_launcher trigger: Processing job {jobid} created")

        message = {
            "recipes": [],
            "parameters": {
                "ispyb_process": jobid,
                "pipeline": pipeline,
                "path_ext": path_ext,
                "shelxc_path": shelxc_path,
                "fast_ep_path": fast_ep_path,
                "msg": rw.payload,
            },
        }
        rw.transport.send("processing_recipe", message)

        self.log.info(f"big_ep_launcher trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    def trigger_big_ep(self, rw, header, parameters, session, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("big_ep trigger failed: No DCID specified")
            return False

        diffraction_plan_info = parameters("diffraction_plan_info")
        try:
            anom_scatterer = diffraction_plan_info["anomalousScatterer"]
            if not anom_scatterer:
                self.log.info(
                    "Skipping big_ep trigger: No anomalous scatterer specified"
                )
                return {"success": True}
        except Exception:
            self.log.info(
                "Skipping big_ep trigger: Cannot read anomalous scatterer setting"
            )
            return {"success": True}

        query = (
            session.query(Proposal, BLSession)
            .join(BLSession, BLSession.proposalId == Proposal.proposalId)
            .join(DataCollection, DataCollection.SESSIONID == BLSession.sessionId)
            .filter(DataCollection.dataCollectionId == dcid)
        )
        proposal = query.first()
        if not proposal:
            self.log.error(
                f"big_ep trigger failed: no proposal associated with dcid={dcid}"
            )
            return False

        if proposal.Proposal.proposalCode in ("lb", "in", "sw"):
            self.log.info(
                f"Skipping big_ep trigger for {proposal.Proposal.proposalCode} visit"
            )
            return {"success": True}

        try:
            program_id = int(parameters("program_id"))
        except (TypeError, ValueError):
            self.log.error("big_ep trigger failed: Invalid program_id specified")
            return False
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
        )
        big_ep_params = None
        for app in query.all():
            if app.autoProcProgramId == program_id:
                if (
                    proposal.BLSession.beamLineName == "i23"
                    and "multi" not in app.processingPrograms
                ):
                    self.log.info(
                        f"Skipping big_ep trigger for {app.processingPrograms} data on i23"
                    )
                    return {"success": True}
                big_ep_params = parameters(app.processingPrograms)
                break
        try:
            assert big_ep_params
        except (AssertionError, NameError):
            self.log.error(
                "big_ep trigger failed: No input data provided for program %s",
                app.processingPrograms,
            )
            return False
        data = big_ep_params["data"]
        if not data:
            self.log.error("big_ep trigger failed: No input data file specified")
            return False
        scaled_unmerged_mtz = big_ep_params["scaled_unmerged_mtz"]
        if not scaled_unmerged_mtz:
            self.log.error(
                "big_ep trigger failed: No input scaled unmerged mtz file specified"
            )
            return False
        path_ext = big_ep_params["path_ext"]
        if not path_ext:
            path_ext = datetime.now().strftime("%Y%m%d_%H%M%S")

        spacegroup = parameters("spacegroup")
        if spacegroup:
            path_ext += "-" + spacegroup

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = bool(parameters("automatic"))
        jp["comments"] = parameters("comment")
        jp["datacollectionid"] = dcid
        jp["display_name"] = "big_ep"
        jp["recipe"] = "postprocessing-big-ep"
        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.debug(f"big_ep trigger: generated JobID {jobid}")

        big_ep_parameters = {
            "program_id": program_id,
            "data": data,
            "scaled_unmerged_mtz": scaled_unmerged_mtz,
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
                "program_id": program_id,
                "data": data,
                "scaled_unmerged_mtz": scaled_unmerged_mtz,
                "path_ext": path_ext,
            },
            "recipes": [],
        }
        rw.transport.send("processing_recipe", message)

        self.log.info("big_ep triggered")

        return {"success": True, "return_value": None}

    def trigger_multiplex(
        self, rw, header, message, parameters, session, transaction, **kwargs
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
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("xia2.multiplex trigger failed: No DCID specified")
            return False

        dcid = int(dcid)
        wavelength = float(parameters("wavelength"))
        ispyb_params = parameters("ispyb_parameters")
        spacegroup = ispyb_params.get("spacegroup") if ispyb_params else None

        # Take related dcids from recipe in preference
        related_dcids = parameters("related_dcids")
        self.log.info(f"related_dcids={related_dcids}")

        if not related_dcids:
            self.log.debug(f"No related_dcids for dcid={dcid}")
            return {"success": True}

        self.log.debug(f"related_dcids for dcid={dcid}: {related_dcids}")

        # Calculate message delay for exponential backoff in case a processing
        # program for a related data collection is still running, in which case
        # we checkpoint with the calculated message delay
        delay_base = rw.recipe_step["parameters"].get("backoff-delay", 8)
        max_try = rw.recipe_step["parameters"].get("backoff-max-try", 10)
        delay_multiplier = rw.recipe_step["parameters"].get("backoff-multiplier", 2)
        status = {
            "ntry": 0,
        }
        if isinstance(message, dict):
            status.update(message.get("trigger-status", {}))
        message_delay = delay_base * delay_multiplier ** status["ntry"]
        status["ntry"] += 1
        self.log.debug(f"dcid={dcid}\nmessage_delay={message_delay}\n{status}")

        multiplex_job_dcids = []
        jobids = []

        for group in related_dcids:
            self.log.debug(f"group: {group}")
            # Select only those dcids that were collected before the triggering dcid
            dcids = [d for d in group["dcids"] if d < dcid]

            # Add the current dcid at the beginning of the list
            dcids.insert(0, dcid)

            if len(dcids) == 1:
                self.log.info(
                    f"Skipping xia2.multiplex trigger: no related dcids for dcid={dcid} group={group}"
                )
                continue
            self.log.info(f"xia2.multiplex trigger: found dcids: {dcids}")

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
                .filter(AutoProcProgram.processingStatus != 0)
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
                if wavelength and dc.wavelength != wavelength:
                    self.log.debug(
                        f"Discarding appid {app.autoProcProgramId} (wavelength does not match input):\n"
                        f"    {dc.wavelength} != {wavelength}"
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
                if spacegroup and (
                    not job_spacegroup_param or job_spacegroup_param != spacegroup
                ):
                    self.log.debug(f"Discarding appid {app.autoProcProgramId}")
                    continue
                elif job_spacegroup_param and not spacegroup:
                    self.log.debug(f"Discarding appid {app.autoProcProgramId}")
                    continue

                # Check for any programs that are yet to finish (or fail)
                if app.processingStatus != 1 or not app.processingStartTime:
                    if status["ntry"] >= max_try:
                        # Give up waiting for this program to finish and trigger
                        # multiplex with remaining related results are available
                        self.log.info(
                            f"max-try exceeded, giving up waiting for dcid={dcid}\n"
                            f"{app.autoProcProgramId}"
                        )
                        break
                    # Send results to myself for next round of processing
                    self.log.debug(
                        f"Waiting for dcid={dc.dataCollectionId}\nappid={app.autoProcProgramId}"
                    )
                    rw.checkpoint(
                        {"trigger-status": status},
                        delay=message_delay,
                        transaction=transaction,
                    )
                    return {"success": True}

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

            if set(dcids) in multiplex_job_dcids:
                continue
            multiplex_job_dcids.append(set(dcids))

            jp = self.ispyb.mx_processing.get_job_params()
            jp["automatic"] = bool(parameters("automatic"))
            jp["comments"] = parameters("comment")
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

            for k in ("sample_id", "sample_group_id"):
                if k in group:
                    jpp = self.ispyb.mx_processing.get_job_parameter_params()
                    jpp["job_id"] = jobid
                    jpp["parameter_key"] = k
                    jpp["parameter_value"] = group[k]
                    jppid = self.ispyb.mx_processing.upsert_job_parameter(
                        list(jpp.values())
                    )
                    self.log.debug(
                        f"xia2.multiplex trigger: generated JobParameterID {jppid} {k}={group[k]}"
                    )
                    break

            for files in data_files:
                jpp = self.ispyb.mx_processing.get_job_parameter_params()
                jpp["job_id"] = jobid
                jpp["parameter_key"] = "data"
                jpp["parameter_value"] = ";".join(files)
                jppid = self.ispyb.mx_processing.upsert_job_parameter(
                    list(jpp.values())
                )
                self.log.debug(
                    "xia2.multiplex trigger generated JobParameterID {} with files:\n%s".format(
                        jppid
                    ),
                    "\n".join(files),
                )
            if spacegroup:
                jpp = self.ispyb.mx_processing.get_job_parameter_params()
                jpp["job_id"] = jobid
                jpp["parameter_key"] = "spacegroup"
                jpp["parameter_value"] = spacegroup
                jppid = self.ispyb.mx_processing.upsert_job_parameter(
                    list(jpp.values())
                )
                self.log.debug(
                    "xia2.multiplex trigger generated JobParameterID {} with %s=%s".format(
                        jppid
                    ),
                    jpp["parameter_key"],
                    spacegroup,
                )

            message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
            rw.transport.send("processing_recipe", message)

            self.log.info(f"xia2.multiplex trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobids}
