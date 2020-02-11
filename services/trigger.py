from __future__ import absolute_import, division, print_function

import hashlib
import re

import ispyb
import procrunner
import py.path
import six
import workflows.recipe
from workflows.services.common_service import CommonService
from datetime import datetime


class DLSTrigger(CommonService):
    """A service that creates and runs downstream processing jobs.
  """

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
                or not isinstance(base_value, six.string_types)
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
                sha1 = hashlib.sha1(pdb.rawfile).hexdigest()
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
        self.log.info("PDB files: %s", ", ".join(pdb_files))

        if not pdb_files:
            self.log.info(
                "Skipping dimple trigger: DCID has no associated PDB information"
            )
            return {"success": True}

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
        jobid = self.ispyb.mx_processing.upsert_job(jp.values())
        self.log.debug("Dimple trigger: generated JobID {}".format(jobid))

        for key, values in dimple_parameters.items():
            for value in values:
                jpp = self.ispyb.mx_processing.get_job_parameter_params()
                jpp["job_id"] = jobid
                jpp["parameter_key"] = key
                jpp["parameter_value"] = value
                jppid = self.ispyb.mx_processing.upsert_job_parameter(jpp.values())
                self.log.debug(
                    "Dimple trigger: generated JobParameterID {}".format(jppid)
                )

        jisp["job_id"] = jobid
        jispid = self.ispyb.mx_processing.upsert_job_image_sweep(jisp.values())
        self.log.debug("Dimple trigger: generated JobImageSweepID {}".format(jispid))

        self.log.debug("Dimple trigger: Processing job {} created".format(jobid))

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info("Dimple trigger: Processing job {} triggered".format(jobid))

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
        jobid = self.ispyb.mx_processing.upsert_job(jp.values())
        self.log.debug("ep_predict trigger: generated JobID {}".format(jobid))

        ep_parameters = {
            "data": parameters("data"),
            "threshold": parameters("threshold"),
        }

        for key, value in ep_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(jpp.values())
            self.log.debug(
                "ep_predict trigger: generated JobParameterID {}".format(jppid)
            )

        jisp["job_id"] = jobid
        jispid = self.ispyb.mx_processing.upsert_job_image_sweep(jisp.values())
        self.log.debug(
            "ep_predict trigger: generated JobImageSweepID {}".format(jispid)
        )

        self.log.debug("ep_predict trigger: Processing job {} created".format(jobid))

        message = {
            "parameters": {
                "ispyb_process": jobid,
                "data": parameters("data"),
                "threshold": parameters("threshold"),
            },
            "recipes": [],
        }
        rw.transport.send("processing_recipe", message)

        self.log.info("ep_predict trigger: Processing job {} triggered".format(jobid))

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
        jobid = self.ispyb.mx_processing.upsert_job(jp.values())
        self.log.debug("fast_ep trigger: generated JobID {}".format(jobid))

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
            jppid = self.ispyb.mx_processing.upsert_job_parameter(jpp.values())
            self.log.debug("fast_ep trigger: generated JobParameterID {}".format(jppid))

        jisp["job_id"] = jobid
        jispid = self.ispyb.mx_processing.upsert_job_image_sweep(jisp.values())
        self.log.debug("fast_ep trigger: generated JobImageSweepID {}".format(jispid))

        self.log.debug("fast_ep trigger: Processing job {} created".format(jobid))

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info("fast_ep trigger: Processing job {} triggered".format(jobid))

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
        jobid = self.ispyb.mx_processing.upsert_job(jp.values())
        self.log.debug("mrbump trigger: generated JobID {}".format(jobid))

        mrbump_parameters = {
            "hklin": parameters("hklin"),
            "scaling_id": parameters("scaling_id"),
        }

        for key, value in mrbump_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(jpp.values())
            self.log.debug("fast_ep trigger: generated JobParameterID {}".format(jppid))

        self.log.debug("mrbump trigger: Processing job {} created".format(jobid))

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info("mrbump trigger: Processing job {} triggered".format(jobid))

        return {"success": True, "return_value": jobid}

    def trigger_big_ep_launcher(self, rw, header, parameters, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("big_ep_launcher trigger failed: No DCID specified")
            return False

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = bool(parameters("automatic"))
        jp["comments"] = parameters("comment")
        jp["datacollectionid"] = dcid
        jp["display_name"] = "big_ep"
        jp["recipe"] = "postprocessing-big-ep-launcher"
        jobid = self.ispyb.mx_processing.upsert_job(jp.values())
        self.log.debug("big_ep_launcher trigger: generated JobID {}".format(jobid))

        try:
            program_id = int(parameters("program_id"))
        except (TypeError, ValueError):
            self.log.error(
                "big_ep_launcher trigger failed: Invalid program_id specified"
            )
            return False
        mtz = parameters("mtz")
        if not mtz:
            self.log.error(
                "big_ep_launcher trigger failed: No input mtz file specified"
            )
            return False
        path_ext = parameters("path_ext")
        if not path_ext:
            path_ext = datetime.now().strftime("%Y%m%d_%H%M%S")

        big_ep_parameters = {
            "ispyb_autoprocprogramid": program_id,
            "ispyb_dcid": dcid,
            "mtz": mtz,
            "path_ext": path_ext,
        }

        for key, value in big_ep_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(jpp.values())
            self.log.debug(
                "big_ep_laucher trigger: generated JobParameterID {}".format(jppid)
            )

        self.log.debug(
            "big_ep_launcher trigger: Processing job {} created".format(jobid)
        )

        message = {
            "recipes": [],
            "parameters": {
                "ispyb_process": jobid,
                "ispyb_autoprocprogramid": program_id,
                "ispyb_dcid": dcid,
                "mtz": mtz,
                "path_ext": path_ext,
            },
        }
        rw.transport.send("processing_recipe", message)

        self.log.info(
            "big_ep_launcher trigger: Processing job {} triggered".format(jobid)
        )

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
        mtz = big_ep_params["mtz"]
        if not mtz:
            self.log.error("big_ep trigger failed: No input mtz file specified")
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

        message = {
            "parameters": {
                "ispyb_autoprocprogramid": program_id,
                "ispyb_dcid": dcid,
                "mtz": mtz,
                "scaled_unmerged_mtz": scaled_unmerged_mtz,
                "path_ext": path_ext,
            },
            "recipes": ["postprocessing-big-ep"],
        }
        rw.transport.send("processing_recipe", message)

        self.log.info("big_ep triggered")

        return {"success": True, "return_value": None}

    def trigger_multiplex(self, rw, header, parameters, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("xia2.multiplex trigger failed: No DCID specified")
            return False

        # lookup related dcids and exit early if none found
        dcid = int(dcid)
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
            return False
        dcids = [int(d) for d in result["stdout"].split()]

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
                "Skipping xia2.multiplex trigger: no related dcids for dcid %s" % dcid
            )
            return {"success": True}
        self.log.info("xia2.multiplex trigger: found dcids: %s", str(dcids))

        def get_data_files_for_dcid(dcid):
            appid = {}
            dc = self.ispyb.get_data_collection(dcid)
            for intgr in dc.integrations:
                prg = intgr.program
                if (prg.message != "processing successful") or (
                    prg.name != "xia2 dials"
                ):
                    continue
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
            self.log.debug("Retrieving program attachment for appid %s", appid)
            try:
                attachments = self.ispyb.mx_processing.retrieve_program_attachments_for_program_id(
                    appid
                )
            except ispyb.NoResult:
                self.log.warning(
                    "Expected to find exactly 2 data files for appid %s (no files found)",
                    appid,
                )
                return []
            for item in attachments:
                if item["fileType"] == "Result":
                    if (
                        item["fileName"].endswith(
                            ("experiments.json", "reflections.pickle", ".expt", ".refl")
                        )
                        and "_scaled." not in item["fileName"]
                    ):
                        data_files.append(
                            py.path.local(item["filePath"])
                            .join(item["fileName"])
                            .strpath
                        )
            self.log.debug(
                "Found the following files for appid %s:\n%s",
                appid,
                ", ".join(data_files),
            )
            if len(data_files) != 2:
                self.log.warning(
                    "Expected to find exactly 2 data files for appid %s (found %s)",
                    appid,
                    len(data_files),
                )
                return []
            return data_files

        # Lookup appids for all dcids and exit early if only one found
        data_files = [get_data_files_for_dcid(d) for d in dcids]
        # Select only those dcids with a valid data files
        dcids, data_files = zip(
            *((dcid, files) for dcid, files in zip(dcids, data_files) if files)
        )
        self.log.info(data_files)
        if len(data_files) <= 1:
            self.log.info(
                "Skipping xia2.multiplex trigger: not enough related data files found for dcid %s"
                % dcid
            )
            return {"success": True}

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = bool(parameters("automatic"))
        jp["comments"] = parameters("comment")
        jp["datacollectionid"] = dcid
        jp["display_name"] = "xia2.multiplex"
        jp["recipe"] = "postprocessing-xia2-multiplex"
        jobid = self.ispyb.mx_processing.upsert_job(jp.values())
        self.log.debug("xia2.multiplex trigger: generated JobID {}".format(jobid))

        for d in dcids:
            dc_info = self.ispyb.get_data_collection(d)

            jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
            jisp["datacollectionid"] = d
            jisp["start_image"] = dc_info.image_start_number
            jisp["end_image"] = dc_info.image_start_number + dc_info.image_count - 1

            jisp["job_id"] = jobid
            jispid = self.ispyb.mx_processing.upsert_job_image_sweep(jisp.values())
            self.log.debug(
                "xia2.multiplex trigger: generated JobImageSweepID {}".format(jispid)
            )

        for files in data_files:
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = "data"
            jpp["parameter_value"] = ";".join(files)
            jppid = self.ispyb.mx_processing.upsert_job_parameter(jpp.values())
            self.log.debug(
                "xia2.multiplex trigger generated JobParameterID {} with files:\n%s".format(
                    jppid
                ),
                "\n".join(files),
            )

        self.log.debug(
            "xia2.multiplex trigger: Processing job {} created".format(jobid)
        )

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info(
            "xia2.multiplex trigger: Processing job {} triggered".format(jobid)
        )

        return {"success": True, "return_value": jobid}
