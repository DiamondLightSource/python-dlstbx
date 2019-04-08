from __future__ import absolute_import, division, print_function

import os.path
import time

import dlstbx.util.gda
import ispyb
import mysql.connector
import six
import workflows.recipe
from workflows.services.common_service import CommonService


class DLSISPyB(CommonService):
    """A service that receives information to be written to ISPyB."""

    # Human readable service name
    _service_name = "DLS ISPyB connector"

    # Logger name
    _logger_name = "dlstbx.services.ispyb"

    def initializing(self):
        """Subscribe the ISPyB connector queue. Received messages must be
       acknowledged. Prepare ISPyB database connection."""
        self.log.info("ISPyB connector using ispyb v%s", ispyb.__version__)
        self.ispyb = ispyb.open("/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg")
        self.log.debug("ISPyB connector starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "ispyb_connector",  # will become 'ispyb' in far future
            self.receive_msg,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def receive_msg(self, rw, header, message):
        """Do something with ISPyB."""

        if header.get("redelivered") == "true":
            # A redelivered message may just have been processed in a parallel instance,
            # which was connected to a different database server in the DB cluster. If
            # we were to process it immediately we may run into a DB synchronization
            # fault. Avoid this by giving the DB cluster a bit of time to settle.
            self.log.debug("Received redelivered message, holding for a second.")
            time.sleep(1)

        if not rw:
            # Incoming message is not a recipe message. Simple messages can be valid
            if (
                not isinstance(message, dict)
                or not message.get("parameters")
                or not message.get("content")
            ):
                self.log.error("Rejected invalid simple message")
                self._transport.nack(header)
                return
            self.log.debug("Received a simple message")

            # Create a wrapper-like object that can be passed to functions
            # as if a recipe wrapper was present.
            class RW_mock(object):
                def dummy(self, *args, **kwargs):
                    pass

            rw = RW_mock()
            rw.transport = self._transport
            rw.recipe_step = {"parameters": message["parameters"]}
            rw.environment = {"has_recipe_wrapper": False}
            rw.set_default_channel = rw.dummy
            rw.send = rw.dummy
            message = message["content"]

        command = rw.recipe_step["parameters"].get("ispyb_command")
        if not command:
            self.log.error("Received message is not a valid ISPyB command")
            rw.transport.nack(header)
            return
        if not hasattr(self, "do_" + command):
            self.log.error("Received unknown ISPyB command (%s)", command)
            rw.transport.nack(header)
            return

        txn = rw.transport.transaction_begin()
        rw.set_default_channel("output")

        def parameters(parameter, replace_variables=True):
            if isinstance(message, dict):
                base_value = message.get(
                    parameter, rw.recipe_step["parameters"].get(parameter)
                )
            else:
                base_value = rw.recipe_step["parameters"].get(parameter)
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

        result = getattr(self, "do_" + command)(
            rw=rw, message=message, parameters=parameters, transaction=txn
        )

        store_result = rw.recipe_step["parameters"].get("store_result")
        if store_result and result and "return_value" in result:
            rw.environment[store_result] = result["return_value"]
            self.log.debug(
                "Storing result '%s' in environment variable '%s'",
                result["return_value"],
                store_result,
            )
        if result and result.get("success"):
            rw.send({"result": result.get("return_value")}, transaction=txn)
            rw.transport.ack(header, transaction=txn)
        elif result and result.get("checkpoint"):
            rw.checkpoint(
                result.get("return_value"),
                delay=rw.recipe_step["parameters"].get("delay"),
                transaction=txn,
            )
            rw.transport.ack(header, transaction=txn)
        else:
            rw.transport.transaction_abort(txn)
            rw.transport.nack(header)
            return
        rw.transport.transaction_commit(txn)

    def do_update_processing_status(self, parameters, **kwargs):
        ppid = parameters("program_id")
        message = parameters("message")
        status = parameters("status")
        try:
            result = self.ispyb.mx_processing.upsert_program(
                program_id=ppid,
                status={"success": 1, "failure": 0}.get(status),
                time_start=parameters("start_time"),
                time_update=parameters("update_time"),
                message=message,
            )
            self.log.info(
                "Updating program %s status: '%s' with result %s", ppid, message, result
            )
            return {"success": True, "return_value": result}
        except ispyb.ISPyBException as e:
            self.log.error(
                "Updating program %s status: '%s' caused exception '%s'.",
                ppid,
                message,
                e,
                exc_info=True,
            )
            return False

    def do_store_dimple_failure(self, parameters, **kwargs):
        params = self.ispyb.mx_processing.get_run_params()
        params["parentid"] = parameters("scaling_id")
        params["pipeline"] = "dimple"
        params["success"] = 0
        params["message"] = "Unknown error"
        params["run_dir"] = parameters("directory")
        try:
            result = self.ispyb.mx_processing.upsert_run(params.values())
            return {"success": True, "return_value": result}
        except ispyb.ISPyBException as e:
            self.log.error(
                "Updating DIMPLE failure for %s caused exception '%s'.",
                params["parentid"],
                e,
                exc_info=True,
            )
            return False

    def do_register_processing(self, parameters, **kwargs):
        program = parameters("program")
        cmdline = parameters("cmdline")
        environment = parameters("environment")
        if isinstance(environment, dict):
            environment = ", ".join(
                "%s=%s" % (key, value) for key, value in environment.items()
            )
        rpid = parameters("rpid")
        if rpid and not rpid.isdigit():
            self.log.error("Invalid processing id '%s'" % rpid)
            return False
        try:
            result = self.ispyb.mx_processing.upsert_program(
                job_id=rpid, name=program, command=cmdline, environment=environment
            )
            self.log.info(
                "Registered new program '%s' for processing id '%s' with command line '%s' and environment '%s' with result '%s'.",
                program,
                rpid,
                cmdline,
                environment,
                result,
            )
            return {"success": True, "return_value": result}
        except ispyb.ISPyBException as e:
            self.log.error(
                "Registering new program '%s' for processing id '%s' with command line '%s' and environment '%s' caused exception '%s'.",
                program,
                rpid,
                cmdline,
                environment,
                e,
                exc_info=True,
            )
            return False

    def do_add_program_attachment(self, parameters, **kwargs):
        params = self.ispyb.mx_processing.get_program_attachment_params()
        params["parentid"] = parameters("program_id")
        try:
            programid = int(params["parentid"])
        except ValueError:
            programid = None
        if not programid:
            self.log.warning("Encountered invalid program ID '%s'", params["parentid"])
            return False
        params["file_name"] = parameters("file_name", replace_variables=False)
        params["file_path"] = parameters("file_path", replace_variables=False)
        fqpn = os.path.join(params["file_path"], params["file_name"])

        if not os.path.isfile(fqpn):
            self.log.error(
                "Not adding attachment '%s' to data processing: File does not exist",
                str(fqpn),
            )
            return False

        params["file_type"] = str(parameters("file_type")).lower()
        if params["file_type"] not in ("log", "result", "graph"):
            self.log.warning(
                "Attachment type '%s' unknown, defaulting to 'log'", params["file_type"]
            )
            params["file_type"] = "log"

        self.log.debug("Writing program attachment to database: %s", params)

        result = self.ispyb.mx_processing.upsert_program_attachment(
            list(params.values())
        )
        return {"success": True, "return_value": result}

    def do_add_datacollection_attachment(self, parameters, **kwargs):
        params = self.ispyb.mx_acquisition.get_data_collection_file_attachment_params()

        params["parentid"] = parameters("dcid")
        file_name = parameters("file_name", replace_variables=False)
        file_path = parameters("file_path", replace_variables=False)
        params["file_full_path"] = os.path.join(file_path, file_name)

        if not os.path.isfile(params["file_full_path"]):
            self.log.error(
                "Not adding attachment '%s' to data collection: File does not exist",
                str(params["file_full_path"]),
            )
            return False

        params["file_type"] = str(parameters("file_type")).lower()
        if params["file_type"] not in ("snapshot", "log", "xy", "recip", "pia"):
            self.log.warning(
                "Attachment type '%s' unknown, defaulting to 'log'", params["file_type"]
            )
            params["file_type"] = "log"

        self.log.debug("Writing data collection attachment to database: %s", params)
        result = self.ispyb.mx_acquisition.upsert_data_collection_file_attachment(
            list(params.values())
        )
        return {"success": True, "return_value": result}

    def do_store_per_image_analysis_results(self, parameters, **kwargs):
        params = self.ispyb.mx_processing.get_quality_indicators_params()
        params["datacollectionid"] = parameters("dcid")
        if not params["datacollectionid"]:
            self.log.error("DataCollectionID not specified")
            return False
        params["image_number"] = parameters("file-pattern-index") or parameters(
            "file-number"
        )
        if not params["image_number"]:
            self.log.error("Image number not specified")
            return False

        params["dozor_score"] = parameters("dozor_score")
        params["spot_total"] = parameters("n_spots_total")
        if params["spot_total"] is not None:
            params["in_res_total"] = params["spot_total"]
            params["icerings"] = 0
            params["maxunitcell"] = 0
            params["pctsaturationtop50peaks"] = 0
            params["inresolutionovrlspots"] = 0
            params["binpopcutoffmethod2res"] = 0
        elif params["dozor_score"] is None:
            self.log.error("Message contains neither dozor score nor spot count")
            return False

        params["totalintegratedsignal"] = parameters("total_intensity")
        params["good_bragg_candidates"] = parameters("n_spots_no_ice")
        params["method1_res"] = parameters("estimated_d_min")
        params["method2_res"] = parameters("estimated_d_min")
        params["programid"] = "65228265"  # dummy value

        self.log.debug(
            "Writing PIA record for image %r in DCID %s",
            params["image_number"],
            params["datacollectionid"],
        )

        try:
            #     result = "159956186" # for testing
            result = self._retry_mysql_call(
                self.ispyb.mx_processing.upsert_quality_indicators,
                list(params.values()),
            )
            gdahost = parameters("notify-gda")
            if gdahost:
                if "{" in gdahost:
                    self.log.warning(
                        "Could not notify GDA, %s is not a valid hostname", gdahost
                    )
                elif gdahost == "mx-control":
                    pass  # skip
                elif result is None:
                    self.log.info(
                        "Could not notify GDA, stored procedure returned 'None'"
                    )
                else:
                    # We still notify in the legacy manner by sending a UDP package with DCID+imagenumber
                    try:
                        dlstbx.util.gda.notify(
                            gdahost,
                            9877,
                            "IQI:{p[datacollectionid]}:{p[image_number]}".format(
                                p=params
                            ),
                        )
                    except Exception as e:
                        self.log.warning("Could not notify GDA: %s", e, exc_info=True)
        except ispyb.ReadWriteError as e:
            self.log.error(
                "Could not write PIA results %s to database: %s",
                params,
                e,
                exc_info=True,
            )
            return False
        else:
            return {"success": True, "return_value": result}

    def do_insert_alignment_result(self, parameters, **kwargs):
        phi = parameters("phi")
        chi = parameters("chi")
        kappa = parameters("kappa")
        if phi is None or [chi, kappa].count(None) != 1:
            self.log.error(
                "Invalid axis parameters for alignment results: phi=%s, chi=%s, kappa=%s",
                phi,
                chi,
                kappa,
            )
            return False

        return self.do_insert_screening_result(parameters, **kwargs)

    def do_insert_screening_result(self, parameters, **kwargs):
        mx_screening = self.ispyb.mx_screening

        # screening_params: ['id', 'dcgid', 'dcid', 'programversion', 'shortcomments', 'comments']
        screening_params = mx_screening.get_screening_params()
        screening_params["dcid"] = parameters("dataCollectionId")
        screening_params["program_version"] = parameters("program")
        screening_params["comments"] = parameters("comments") or ""
        screening_params["short_comments"] = parameters("shortComments") or ""
        self.log.info("screening_params: %s", screening_params)
        try:
            screeningId = mx_screening.insert_screening(list(screening_params.values()))
            assert screeningId is not None
        except (ispyb.ISPyBException, AssertionError) as e:
            self.log.error(
                "Inserting screening results: '%s' caused exception '%s'.",
                screening_params,
                e,
                exc_info=True,
            )
            return False

        # output_params: ['id', 'screeningid', 'statusdescription', 'rejectedreflections', 'resolutionobtained', 'spotdeviationr', 'spotdeviationtheta', 'beamshiftx', 'beamshifty', 'numspotsfound', 'numspotsused', 'numspotsrejected', 'mosaicity', 'ioversigma', 'diffractionrings', 'mosaicityestimated', 'rankingresolution', 'program', 'dosetotal', 'totalexposuretime', 'totalrotationrange', 'totalnoimages', 'rfriedel', 'indexingsuccess', 'strategysuccess', 'alignmentsuccess']
        output_params = mx_screening.get_screening_output_params()
        output_params["screening_id"] = screeningId
        output_params["mosaicity"] = parameters("mosaicity")
        output_params["mosaicityEstimated"] = 1 if parameters("mosaicity") else 0
        # output_params["screeningSuccess"] = 1
        output_params["indexingSuccess"] = 1
        output_params["strategySuccess"] = 1
        output_params["program"] = parameters("program") or ""
        output_params["rankingResolution"] = parameters("rankingresolution")
        self.log.info("output_params: %s", output_params)
        try:
            screeningOutputId = mx_screening.insert_screening_output(
                list(output_params.values())
            )
            assert screeningOutputId is not None
        except (ispyb.ISPyBException, AssertionError) as e:
            self.log.error(
                "Inserting screening output: '%s' caused exception '%s'.",
                output_params,
                e,
                exc_info=True,
            )
            return False

        # output_lattice_params ['id', 'screeningoutputid', 'spacegroup', 'pointgroup', 'bravaislattice', 'raworientationmatrixax', 'raworientationmatrixay', 'raworientationmatrixaz', 'raworientationmatrixbx', 'raworientationmatrixby', 'raworientationmatrixbz', 'raworientationmatrixcx', 'raworientationmatrixcy', 'raworientationmatrixcz', 'unitcella', 'unitcellb', 'unitcellc', 'unitcellalpha', 'unitcellbeta', 'unitcellgamma', 'labelitindexing']
        output_lattice_params = mx_screening.get_screening_output_lattice_params()
        output_lattice_params["screening_output_id"] = screeningOutputId
        output_lattice_params["unitcella"] = parameters("unitcella") or ""
        output_lattice_params["unitcellb"] = parameters("unitcellb") or ""
        output_lattice_params["unitcellc"] = parameters("unitcellc") or ""
        output_lattice_params["unitcellalpha"] = parameters("unitcellalpha") or ""
        output_lattice_params["unitcellbeta"] = parameters("unitcellbeta") or ""
        output_lattice_params["unitcellgamma"] = parameters("unitcellgamma") or ""
        output_lattice_params["spacegroup"] = parameters("spacegroup") or ""
        self.log.info("output_lattice_params: %s", output_lattice_params)
        try:
            screeningOutputLatticeId = mx_screening.insert_screening_output_lattice(
                list(output_lattice_params.values())
            )
            assert screeningOutputLatticeId is not None
        except (ispyb.ISPyBException, AssertionError) as e:
            self.log.error(
                "Inserting screening output lattice: '%s' caused exception '%s'.",
                output_lattice_params,
                e,
                exc_info=True,
            )
            return False

        # strategy_params ['id', 'screeningoutputid', 'phistart', 'phiend', 'rotation', 'exposuretime', 'resolution', 'completeness', 'multiplicity', 'anomalous', 'program', 'rankingresolution', 'transmission']
        strategy_params = mx_screening.get_screening_strategy_params()
        strategy_params["screening_output_id"] = screeningOutputId
        strategy_params["program"] = parameters("program")
        strategy_params["anomalous"] = parameters("anomalous") or 0
        strategy_params["transmission"] = parameters("transmission")
        strategy_params["exposureTime"] = parameters("exposuretime")
        self.log.info("strategy_params: %s", strategy_params)
        try:
            screeningStrategyId = mx_screening.insert_screening_strategy(
                list(strategy_params.values())
            )
            assert screeningStrategyId is not None
        except (ispyb.ISPyBException, AssertionError) as e:
            self.log.error(
                "Inserting screening strategy: '%s' caused exception '%s'.",
                strategy_params,
                e,
                exc_info=True,
            )
            return False

        # wedge_params ['id', 'screeningstrategyid', 'wedgenumber', 'resolution', 'completeness', 'multiplicity', 'dosetotal', 'noimages', 'phi', 'kappa', 'chi', 'comments', 'wavelength']
        wedge_params = mx_screening.get_screening_strategy_wedge_params()
        wedge_params["screening_strategy_id"] = screeningStrategyId
        wedge_params["phi"] = parameters("phi")
        wedge_params["chi"] = parameters("chi")
        wedge_params["kappa"] = parameters("kappa")
        wedge_params["wedgeNumber"] = parameters("wedgenumber") or "1"
        wedge_params["resolution"] = parameters("resolution")
        wedge_params["completeness"] = parameters("completeness")
        wedge_params["multiplicity"] = parameters("multiplicity")
        wedge_params["dosetotal"] = parameters("dosetotal")
        wedge_params["noimages"] = parameters("noimages")
        self.log.info("wedge_params: %s", wedge_params)
        try:
            screeningStrategyWedgeId = mx_screening.insert_screening_strategy_wedge(
                list(wedge_params.values())
            )
            assert screeningStrategyWedgeId is not None
        except (ispyb.ISPyBException, AssertionError) as e:
            self.log.error(
                "Inserting strategy wedge: '%s' caused exception '%s'.",
                wedge_params,
                e,
                exc_info=True,
            )
            return False

        # sub_wedge_params ['id', 'screeningstrategywedgeid', 'subwedgenumber', 'rotationaxis', 'axisstart', 'axisend', 'exposuretime', 'transmission', 'oscillationrange', 'completeness', 'multiplicity', 'resolution', 'dosetotal', 'noimages', 'comments']
        sub_wedge_params = mx_screening.get_screening_strategy_sub_wedge_params()
        sub_wedge_params["screening_strategy_wedge_id"] = screeningStrategyWedgeId
        sub_wedge_params["subwedgenumber"] = "1"
        sub_wedge_params["rotationaxis"] = "omega"
        sub_wedge_params["axisstart"] = parameters("phistart")
        sub_wedge_params["axisend"] = parameters("phiend")
        sub_wedge_params["exposuretime"] = parameters("exposuretime")
        sub_wedge_params["transmission"] = parameters("transmission")
        sub_wedge_params["oscillationrange"] = parameters("oscillationrange")
        sub_wedge_params["completeness"] = parameters("completeness")
        sub_wedge_params["multiplicity"] = parameters("multiplicity")
        sub_wedge_params["resolution"] = parameters("resolution")
        sub_wedge_params["dosetotal"] = parameters("dosetotal")
        sub_wedge_params["noimages"] = parameters("noimages")
        self.log.info("sub_wedge_params: %s", sub_wedge_params)
        try:
            screeningStrategySubWedgeId = mx_screening.insert_screening_strategy_sub_wedge(
                list(sub_wedge_params.values())
            )
            assert screeningStrategySubWedgeId is not None
        except (ispyb.ISPyBException, AssertionError) as e:
            self.log.error(
                "Inserting strategy sub wedge: '%s' caused exception '%s'.",
                sub_wedge_params,
                e,
                exc_info=True,
            )
            return False
        self.log.info(
            "Inserted screening results with IDs %s, %s, %s, %s, %s, %s",
            str(screeningId),
            str(screeningOutputId),
            str(screeningOutputLatticeId),
            str(screeningStrategyId),
            str(screeningStrategyWedgeId),
            str(screeningStrategySubWedgeId),
        )
        return {"success": True}

    def do_register_integration(self, **kwargs):
        # deprecated
        return self.do_upsert_integration(**kwargs)

    def do_upsert_integration(self, parameters, **kwargs):
        """Insert or update an AutoProcIntegration record.

       Parameters, amongst others defined by the ISPyB API:
       :dcid: related DataCollectionID
       :integration_id: AutoProcIntegrationID, if defined will UPDATE otherwise INSERT
       :program_id: related AutoProcProgramID
       :scaling_id: related AutoProcScalingID

       :returns: AutoProcIntegrationID

       ISPyB-API call: upsert_integration
    """
        self.log.info(
            "Saving integration result record (%s) for DCID %s and APPID %s",
            parameters("integration_id") or "new",
            parameters("dcid"),
            parameters("program_id"),
        )
        params = self.ispyb.mx_processing.get_integration_params()
        params["datacollectionid"] = parameters("dcid")
        params["id"] = parameters("integration_id")
        params["parentid"] = parameters("scaling_id")
        params["programid"] = parameters("program_id")
        for key in (
            "anom",
            "beam_vec_x",
            "beam_vec_y",
            "beam_vec_z",
            "cell_a",
            "cell_b",
            "cell_c",
            "cell_alpha",
            "cell_beta",
            "cell_gamma",
            "start_image_no",
            "end_image_no",
            "refined_detector_dist",
            "refined_xbeam",
            "refined_ybeam",
            "rot_axis_x",
            "rot_axis_y",
            "rot_axis_z",
        ):
            params[key] = parameters(key)

        try:
            autoProcIntegrationId = self.ispyb.mx_processing.upsert_integration(
                list(params.values())
            )
            assert autoProcIntegrationId is not None
        except (ispyb.ISPyBException, AssertionError) as e:
            self.log.error(
                "Encountered exception %s when attempting to insert/update integration record '%s'",
                e,
                params,
                exc_info=True,
            )
            return False
        self.log.info("Saved integration record ID %s", autoProcIntegrationId)
        return {"success": True, "return_value": autoProcIntegrationId}

    def do_write_autoproc(self, parameters, **kwargs):
        """Write entry to the AutoProc table."""
        params = self.ispyb.mx_processing.get_processing_params()
        params["id"] = parameters("autoproc_id")  # will create a new record
        # if undefined
        params["parentid"] = parameters("program_id")
        for key in (
            "spacegroup",
            "refinedcell_a",
            "refinedcell_b",
            "refinedcell_c",
            "refinedcell_alpha",
            "refinedcell_beta",
            "refinedcell_gamma",
        ):
            params[key] = parameters(key)
        try:
            autoProcId = self.ispyb.mx_processing.upsert_processing(
                list(params.values())
            )
            assert autoProcId is not None
        except (ispyb.ISPyBException, AssertionError) as e:
            self.log.error(
                "Writing AutoProc record '%s' caused exception '%s'.",
                params,
                e,
                exc_info=True,
            )
            return False
        self.log.info("Written AutoProc record with ID %s", autoProcId)
        return {"success": True, "return_value": autoProcId}

    def do_insert_scaling(self, parameters, **kwargs):
        """Write a 3-column scaling statistics table to the database.

       Parameters:
       :autoproc_id: AutoProcId, key to AutoProc table
       :outerShell: dictionary containing scaling statistics
       :innerShell: dictionary containing scaling statistics
       :overall: dictionary containing scaling statistics

       :returns: AutoProcScalingId

       ISPyB-API call: insert_scaling
    """
        autoProcId = parameters("autoproc_id")
        stats = {
            "outerShell": self.ispyb.mx_processing.get_outer_shell_scaling_params(),
            "innerShell": self.ispyb.mx_processing.get_inner_shell_scaling_params(),
            "overall": self.ispyb.mx_processing.get_overall_scaling_params(),
        }
        for shell in stats:
            for key in (
                "anom",
                "anom_completeness",
                "anom_multiplicity",
                "cc_anom",
                "cc_half",
                "comments",
                "completeness",
                "fract_partial_bias",
                "mean_i_sig_i",
                "multiplicity",
                "n_tot_obs",
                "n_tot_unique_obs",
                "r_meas_all_iplusi_minus",
                "r_meas_within_iplusi_minus",
                "r_merge",
                "r_pim_all_iplusi_minus",
                "r_pim_within_iplusi_minus",
                "res_lim_high",
                "res_lim_low",
            ):
                stats[shell][key] = parameters(shell).get(key)
        try:
            scalingId = self.ispyb.mx_processing.insert_scaling(
                autoProcId,
                list(stats["outerShell"].values()),
                list(stats["innerShell"].values()),
                list(stats["overall"].values()),
            )
            assert scalingId is not None
        except (ispyb.ISPyBException, AssertionError) as e:
            self.log.error(
                "Encountered exception %s when attempting to insert scaling "
                "statistics '%s' for AutoProcId %s",
                e,
                stats,
                autoProcId,
                exc_info=True,
            )
            return False
        self.log.info(
            "Written scaling statistics record %s for AutoProc ID %s",
            scalingId,
            autoProcId,
        )
        return {"success": True, "return_value": scalingId}

    def do_multipart_message(self, rw, message, **kwargs):
        """The multipart_message command allows the recipe or client to specify a
       multi-stage operation. With this you can process a list of API calls,
       for example
         * do_upsert_processing
         * do_insert_scaling
         * do_upsert_integration
       Each API call may have a return value that can be stored.
       Multipart_message takes care of chaining and checkpointing to make the
       overall call near-ACID compliant."""

        if not rw.environment.get("has_recipe_wrapper", True):
            self.log.error(
                "Multipart message call can not be used with simple messages"
            )
            return False

        checkpoint = 1
        commands = rw.recipe_step["parameters"].get("ispyb_command_list")
        if isinstance(message, dict) and isinstance(
            message.get("ispyb_command_list"), list
        ):
            commands = message["ispyb_command_list"]
            checkpoint = message.get("checkpoint", 0) + 1
        if not commands:
            self.log.error("Received multipart message containing no commands")
            return False

        current_command = commands.pop(0)
        command = current_command.get("ispyb_command")
        if not command:
            self.log.error(
                "Multipart command %s is not a valid ISPyB command", current_command
            )
            return False
        if not hasattr(self, "do_" + command):
            self.log.error("Received unknown ISPyB command (%s)", command)
            return False
        self.log.debug(
            "Processing step %d of multipart message (%s) with %d further steps",
            checkpoint,
            command,
            len(commands),
        )

        # Create a parameter lookup function specific to this step of the
        # multipart message
        def parameters(parameter, replace_variables=True):
            """Slight change in behaviour compared to 'parameters' in a direct call:
         If the value is defined in the command list item then this takes
         precedence. Otherwise we check the original message content. Finally
         we look in parameters dictionary of the recipe step for the
         multipart_message command.
         String replacement rules apply as usual."""
            if parameter in current_command:
                base_value = current_command[parameter]
            elif isinstance(message, dict) and parameter in message:
                base_value = message[parameter]
            else:
                base_value = rw.recipe_step["parameters"].get(parameter)
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

        kwargs["parameters"] = parameters

        # Run the multipart step
        result = getattr(self, "do_" + command)(rw=rw, message=message, **kwargs)

        # Store step result if appropriate
        store_result = current_command.get("store_result")
        if store_result and result and "return_value" in result:
            rw.environment[store_result] = result["return_value"]
            self.log.debug(
                "Storing result '%s' in environment variable '%s'",
                result["return_value"],
                store_result,
            )

        # If the step did not succeed then propagate failure
        if not result or not result.get("success"):
            self.log.debug("Multipart command failed")
            return result

        # If the multipart command is finished then propagate success
        if not commands:
            self.log.debug("and done.")
            return result

        # If there are more steps then checkpoint the current state
        # and put it back on the queue
        self.log.debug("Checkpointing remaining %d steps", len(commands))
        if isinstance(message, dict):
            checkpoint_dictionary = message
        else:
            checkpoint_dictionary = {}
        checkpoint_dictionary["checkpoint"] = checkpoint
        checkpoint_dictionary["ispyb_command_list"] = commands
        return {"checkpoint": True, "return_value": checkpoint_dictionary}

    def _retry_mysql_call(self, function, *args, **kwargs):
        tries = 0
        while True:
            try:
                return function(*args, **kwargs)
            except (
                mysql.connector.errors.InternalError,
                mysql.connector.errors.IntegrityError,
            ) as e:
                tries = tries + 1
                if tries < 3:
                    self.log.warning(
                        "ISPyB call %s try %d failed with %s",
                        function,
                        tries,
                        e,
                        exc_info=True,
                    )
                    continue
                else:
                    raise
