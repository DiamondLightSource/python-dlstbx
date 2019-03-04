from __future__ import absolute_import, division, print_function

import ispyb
import logging
import os
import re

import procrunner
import six
import workflows.recipe
from workflows.services.common_service import CommonService


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

        dc_info = self.ispyb.get_data_collection(dcid)
        if not dc_info.pdb:
            self.log.info(
                "Skipping dimple trigger: DCID has no associated PDB information"
            )
            return {"success": True}

        dimple_parameters = {
            "data": parameters("mtz"),
            "scaling_id": parameters("scaling_id"),
        }
        if parameters("set_synchweb_status"):
            dimple_parameters["set_synchweb_status"] = 1

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

        for key, value in dimple_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(jpp.values())
            self.log.debug("Dimple trigger: generated JobParameterID {}".format(jppid))

        jisp["job_id"] = jobid
        jispid = self.ispyb.mx_processing.upsert_job_image_sweep(jisp.values())
        self.log.debug("Dimple trigger: generated JobImageSweepID {}".format(jispid))

        self.log.debug("Dimple trigger: Processing job {} created".format(jobid))

        message = {"recipes": [], "parameters": {"ispyb_process": jobid}}
        rw.transport.send("processing_recipe", message)

        self.log.info("Dimple trigger: Processing job {} triggered".format(jobid))

        return {"success": True, "return_value": jobid}

    def trigger_fast_ep(self, rw, header, parameters, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("fast_ep trigger failed: No DCID specified")
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

    def trigger_big_ep(self, rw, header, parameters, **kwargs):
        dcid = parameters("dcid")
        if not dcid:
            self.log.error("big_ep trigger failed: No DCID specified")
            return False

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

        scaling_id = parameters("scaling_id")
        if not scaling_id:
            self.log.error("big_ep trigger failed: No scaling_id specified")
            return False

        message = {
            "parameters": {"ispyb_autoprocscalingid": scaling_id, "ispyb_dcid": dcid},
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
        result = procrunner.run(
            command,
            # timeout=params.get('timeout'),
            # working_directory=params['working_directory'],
            print_stdout=False,
            print_stderr=False,
        )
        dcids = [int(d) for d in result["stdout"].split()]
        dcids = [dcid] + [d for d in dcids if d < dcid]
        if len(dcids) == 1:
            self.log.info(
                "Skipping xia2.multiplex trigger: no related dcids for dcid %s" % dcid
            )
            return {"success": True}
        self.log.info("xia2.multiplex trigger: found dcids: %s", str(dcids))

        def get_appid(dcid):
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
            return appid.values()[0]

        # lookup appids for all dcids and exit early if only one found
        appids = [get_appid(d) for d in dcids]
        appids = [appid for appid in appids if appid is not None]
        if len(appids) <= 1:
            self.log.info(
                "Skipping xia2.multiplex trigger: not enough related appids found for dcid %s"
                % dcid
            )
            return {"success": True}
        if len(appids) > 100:
            self.log.info(
                "Skipping xia2.multiplex trigger: too many related appids found for dcid %s"
                % dcid
            )
            return {"success": True}
        self.log.info("xia2.multiplex trigger: found appids: %s", str(appids))

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

        multiplex_parameters = {"appids": ",".join(str(a) for a in appids)}

        for key, value in multiplex_parameters.items():
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(jpp.values())
            self.log.debug(
                "xia2.multiplex trigger: generated JobParameterID {}".format(jppid)
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
