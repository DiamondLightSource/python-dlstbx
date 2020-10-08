import hashlib
import re

import ispyb
import procrunner
import py.path
import workflows.recipe
from workflows.services.common_service import CommonService
from datetime import datetime


class DLSTrigger(CommonService):
    """A service that creates and runs downstream processing jobs."""

    # Human readable service name
    _service_name = "DLS Trigger"

    # Logger name
    _logger_name = "dlstbx.services.trigger"

    def initializing(self):
        """Subscribe to the trigger queue. Received messages must be acknowledged."""
        workflows.recipe.wrap_subscribe(
            self._transport,
            "trigger",
            self.trigger,
            acknowledgement=True,
            log_extender=self.extend_log,
        )
        import ispyb.model.__future__

        ispyb.model.__future__.enable(
            "/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg"
        )
        self.ispyb = ispyb.open("/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg")

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

        result = getattr(self, "trigger_" + target)(
            rw=rw,
            header=header,
            message=message,
            parameters=parameters,
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

    def trigger_dimple(self, rw, header, parameters, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("Dimple trigger failed: No DCID specified")
            return False

        pdb_tmpdir = py.path.local(parameters("pdb_tmpdir"))

        pdb_files = []
        dc_info = self.ispyb.get_data_collection(dcid)
        for pdb in dc_info.pdb:
            if pdb.code is not None:
                pdb_files.append(pdb.code)
            elif pdb.rawfile is not None:
                sha1 = hashlib.sha1(pdb.rawfile.encode()).hexdigest()
                assert pdb.name and "/" not in pdb.name, "Invalid PDB file name"
                pdb_filepath = pdb_tmpdir / sha1 / pdb.name
                if not pdb_filepath.check():
                    pdb_filepath.write(pdb.rawfile, ensure=True)
                pdb_files.append(pdb_filepath.strpath)

        if parameters("user_pdb_directory"):
            # Look for matching .pdb files in user directory
            user_pdb_dir = py.path.local(parameters("user_pdb_directory"))
            if user_pdb_dir.check(dir=1):
                for f in user_pdb_dir.listdir():
                    self.log.debug(f.strpath)
                    prefix = f.basename.split(".")[0]
                    if not prefix or f.ext != ".pdb" or not f.check(file=1):
                        continue
                    self.log.info(f.strpath)
                    pdb_files.append(f.strpath)

        if not pdb_files:
            self.log.info(
                "Skipping dimple trigger: DCID %s has no associated PDB information"
                % dcid
            )
            return {"success": True}
        self.log.info("PDB files: %s", ", ".join(pdb_files))

        dimple_parameters = {
            "data": [parameters("mtz")],
            "scaling_id": [parameters("scaling_id")],
            "pdb": pdb_files,
        }
        if parameters("set_synchweb_status"):
            dimple_parameters["set_synchweb_status"] = [1]

        jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
        jisp["datacollectionid"] = dcid
        jisp["start_image"] = dc_info.image_start_number
        jisp["end_image"] = dc_info.image_start_number + dc_info.image_count - 1

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

    def trigger_ep_predict(self, rw, header, parameters, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("ep_predict trigger failed: No DCID specified")
            return False

        diffraction_plan_info = parameters("diffraction_plan_info")
        if not diffraction_plan_info:
            self.log.info(
                "Skipping ep_predict trigger: diffraction plan information not available"
            )
            return {"success": True}
        try:
            anom_scatterer = diffraction_plan_info["anomalousscatterer"]
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

        dc_info = self.ispyb.get_data_collection(dcid)
        jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
        jisp["datacollectionid"] = dcid
        jisp["start_image"] = dc_info.image_start_number
        jisp["end_image"] = dc_info.image_start_number + dc_info.image_count - 1

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

    def trigger_mr_predict(self, rw, header, parameters, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("mr_predict trigger failed: No DCID specified")
            return False

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

    def trigger_screen19_mx(self, rw, header, parameters, **kwargs):
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

    def trigger_fast_ep(self, rw, header, parameters, **kwargs):
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
            anom_scatterer = diffraction_plan_info["anomalousscatterer"]
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

        dc_info = self.ispyb.get_data_collection(dcid)
        jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
        jisp["datacollectionid"] = dcid
        jisp["start_image"] = dc_info.image_start_number
        jisp["end_image"] = dc_info.image_start_number + dc_info.image_count - 1

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

    def trigger_mrbump(self, rw, header, parameters, **kwargs):
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

    def trigger_big_ep_launcher(self, rw, header, parameters, **kwargs):
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

        big_ep_parameters = {
            "pipeline": pipeline,
            "program_id": program_id,
            "data": data,
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
                "msg": rw.payload,
            },
        }
        rw.transport.send("processing_recipe", message)

        self.log.info(f"big_ep_launcher trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobid}

    def trigger_big_ep(self, rw, header, parameters, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("big_ep trigger failed: No DCID specified")
            return False

        diffraction_plan_info = parameters("diffraction_plan_info")
        try:
            anom_scatterer = diffraction_plan_info["anomalousscatterer"]
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

        file_directory = self.ispyb.get_data_collection(dcid).file_directory
        visit_match = re.search(r"/([a-z]{2}[0-9]{4,5}-[0-9]+)/", file_directory)
        try:
            visit = visit_match.group(1)
        except AttributeError:
            self.log.error(
                "big_ep trigger failed: Cannot match visit pattern in path %s",
                file_directory,
            )
            return False
        if True in [pfx in visit for pfx in ("lb", "in", "sw")]:
            self.log.info("Skipping big_ep for %s visit", visit)
            return {"success": True}

        try:
            program_id = int(parameters("program_id"))
        except (TypeError, ValueError):
            self.log.error("big_ep trigger failed: Invalid program_id specified")
            return False
        programs_all = rw.environment["ispyb_programs_all"]
        for prog in programs_all:
            if prog["id"] == program_id:
                big_ep_params = parameters(prog["programs"])
                break
        try:
            assert big_ep_params
        except (AssertionError, NameError):
            self.log.error(
                "big_ep trigger failed: No input data provided for program %s",
                prog["programs"],
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

    def trigger_multiplex(self, rw, header, parameters, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("xia2.multiplex trigger failed: No DCID specified")
            return False

        dcid = int(dcid)
        ispyb_params = parameters("ispyb_parameters")
        spacegroup = ispyb_params.get("spacegroup") if ispyb_params else None

        # Take related dcids from recipe in preference
        related_dcids = parameters("related_dcids")

        if not related_dcids:
            related_dcids = []

        # lookup related dcids and exit early if none found
        command = [
            "/dls_sw/apps/mx-scripts/misc/GetAListOfAssociatedDCOnThisCrystalOrDir.sh",
            "%i" % dcid,
        ]
        result = procrunner.run(command, print_stdout=False, print_stderr=False)
        if result["exitcode"] or result["timeout"]:
            self.log.info("timeout: %s", result["timeout"])
            self.log.debug(result["stdout"])
            self.log.debug(result["stderr"])
            self.log.error(
                "%s failed with exit code %d", " ".join(command), result["exitcode"]
            )
        else:
            related_dcids.append((int(d) for d in result["stdout"].split()))

        # Only unique sets of dcids
        related_dcids = set(tuple(dcids) for dcids in related_dcids)

        self.log.debug(f"related_dcids for dcid={dcid}: {related_dcids}")

        multiplex_job_dcids = []
        jobids = []

        for dcids in related_dcids:
            # Select only those dcids at the same wavelength as the triggering dcid
            wavelength = self.ispyb.get_data_collection(dcid).wavelength
            dcids = [
                d
                for d in dcids
                if self.ispyb.get_data_collection(d).wavelength == wavelength
            ]

            # Select only those dcids that were collected before the triggering dcid
            dcids = [d for d in dcids if d < dcid]

            # Add the current dcid at the beginning of the list
            dcids.insert(0, dcid)

            if len(dcids) == 1:
                self.log.info(
                    f"Skipping xia2.multiplex trigger: no related dcids for dcid {dcid}"
                )
                return {"success": True}
            self.log.info(f"xia2.multiplex trigger: found dcids: {dcids}")

            def get_data_files_for_dcid(dcid):
                appid = {}
                dc = self.ispyb.get_data_collection(dcid)
                for intgr in dc.integrations:
                    prg = intgr.program
                    if (prg.message != "processing successful") or (
                        prg.name != "xia2 dials"
                    ):
                        continue
                    # If this multiplex job was triggered with a spacegroup parameter
                    # then only use xia2-dials autoprocessing results that were
                    # themselves run with a spacegroup parameter. Else only use those
                    # results that weren't run with a space group parameter
                    job = self.ispyb.get_processing_job(prg.job_id)
                    job.load()
                    self.log.debug(job)
                    if not job.automatic:
                        continue
                    job_spacegroup_param = None
                    for param in job.parameters:
                        self.log.debug(param)
                        if param[1].key == "spacegroup":
                            job_spacegroup_param = param[1]
                            break
                    if spacegroup and (
                        not job_spacegroup_param
                        or job_spacegroup_param.value != spacegroup
                    ):
                        self.log.debug(f"Discarding appid {intgr.APPID}")
                        continue
                    elif job_spacegroup_param and not spacegroup:
                        self.log.debug(f"Discarding appid {intgr.APPID}")
                        continue
                    self.log.debug(f"Using appid {intgr.APPID}")
                    appid[prg.time_update] = intgr.APPID
                if not appid:
                    return None
                for appid in appid.values():
                    data_files = get_data_files_for_appid(appid)
                    if data_files:
                        return data_files
                return []

            def get_data_files_for_appid(appid):
                data_files = []
                self.log.debug(f"Retrieving program attachment for appid {appid}")
                try:
                    attachments = self.ispyb.mx_processing.retrieve_program_attachments_for_program_id(
                        appid
                    )
                except ispyb.NoResult:
                    self.log.warning(
                        f"Expected to find exactly 2 data files for appid {appid} (no files found)"
                    )
                    return []
                for item in attachments:
                    if item["fileType"] == "Result":
                        if (
                            item["fileName"].endswith(
                                (
                                    "experiments.json",
                                    "reflections.pickle",
                                    ".expt",
                                    ".refl",
                                )
                            )
                            and "_scaled." not in item["fileName"]
                        ):
                            data_files.append(
                                py.path.local(item["filePath"])
                                .join(item["fileName"])
                                .strpath
                            )
                self.log.debug(
                    f"Found the following files for appid {appid}:\n{', '.join(data_files)}"
                )
                if len(data_files) % 2:
                    self.log.warning(
                        f"Expected to find an even number of  data files for appid {appid} (found {len(data_files)})"
                    )
                    return []
                return data_files

            # Lookup appids for all dcids and exit early if only one found
            data_files = [get_data_files_for_dcid(d) for d in dcids]
            if not any(data_files):
                self.log.info(
                    f"Skipping xia2.multiplex trigger: no related data files found for dcid {dcid}"
                )
                return {"success": True}

            # Select only those dcids with a valid data files
            dcids, data_files = zip(
                *((dcid, files) for dcid, files in zip(dcids, data_files) if files)
            )
            self.log.info(data_files)
            if len(data_files) <= 1:
                self.log.info(
                    f"Skipping xia2.multiplex trigger: not enough related data files found for dcid {dcid}"
                )
                return {"success": True}

            if set(dcids) in multiplex_job_dcids:
                continue
            multiplex_job_dcids.append(set(dcids))

            jp = self.ispyb.mx_processing.get_job_params()
            jp["automatic"] = bool(parameters("automatic"))
            jp["comments"] = parameters("comment")
            jp["datacollectionid"] = dcid
            jp["display_name"] = "xia2.multiplex"
            jp["recipe"] = "postprocessing-xia2-multiplex"
            jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
            jobids.append(jobid)
            self.log.debug(f"xia2.multiplex trigger: generated JobID {jobid}")

            for d in dcids:
                dc_info = self.ispyb.get_data_collection(d)

                jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
                jisp["datacollectionid"] = d
                jisp["start_image"] = dc_info.image_start_number
                jisp["end_image"] = dc_info.image_start_number + dc_info.image_count - 1

                jisp["job_id"] = jobid
                jispid = self.ispyb.mx_processing.upsert_job_image_sweep(
                    list(jisp.values())
                )
                self.log.debug(
                    f"xia2.multiplex trigger: generated JobImageSweepID {jispid}"
                )

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

            self.log.debug(f"xia2.multiplex trigger: Processing job {jobid} created")

            message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
            rw.transport.send("processing_recipe", message)

            self.log.info(f"xia2.multiplex trigger: Processing job {jobid} triggered")

        return {"success": True, "return_value": jobids}
