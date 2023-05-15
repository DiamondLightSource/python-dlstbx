from __future__ import annotations

import json
import os.path
import pathlib
import time
from typing import List

import ispyb.sqlalchemy
import mysql.connector
import pydantic
import sqlalchemy.orm
import workflows.recipe
from ispyb.sqlalchemy import PDB, ProteinHasPDB
from workflows.services.common_service import CommonService

import dlstbx.services.ispybsvc_buffer as buffer
from dlstbx import crud, schemas
from dlstbx.services.ispybsvc_em import EM_Mixin
from dlstbx.util import ChainMapWithReplacement


def lookup_command(command, refclass):
    return getattr(refclass, "do_" + command, None)


class DimpleResult(pydantic.BaseModel):
    mxmrrun: schemas.MXMRRun
    blobs: List[schemas.Blob]
    auto_proc_program: schemas.AutoProcProgram
    attachments: List[schemas.Attachment]


class DLSISPyB(EM_Mixin, CommonService):
    """A service that receives information to be written to ISPyB."""

    # Human readable service name
    _service_name = "DLS ISPyB connector"

    # Logger name
    _logger_name = "dlstbx.services.ispyb"

    def initializing(self):
        """Subscribe the ISPyB connector queue. Received messages must be
        acknowledged. Prepare ISPyB database connection."""
        self.log.info(f"ISPyB connector using ispyb v{ispyb.__version__}")
        self.ispyb = ispyb.open()
        self._ispyb_sessionmaker = sqlalchemy.orm.sessionmaker(
            bind=sqlalchemy.create_engine(
                ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
            )
        )
        try:
            self.log.info("Cleaning up ISPyB buffer table...")
            with self._ispyb_sessionmaker() as session:
                buffer.evict(session=session)
        except Exception as e:
            self.log.warning(
                f"Encountered exception {e!r} while cleaning up ISPyB buffer table",
                exc_info=True,
            )
        self.log.info("ISPyB service ready")
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
            self.log.debug("Received redelivered message, holding for a moment.")
            time.sleep(0.5)

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
            class RW_mock:
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
        command_function = lookup_command(command, self)
        if not command_function:
            self.log.error("Received unknown ISPyB command (%s)", command)
            rw.transport.nack(header)
            return

        self.log.debug("Running ISPyB call %s", command)
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.set_default_channel("output")

        parameter_map = ChainMapWithReplacement(
            message if isinstance(message, dict) else {},
            rw.recipe_step["parameters"],
            substitutions=rw.environment,
        )

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
                or not isinstance(base_value, str)
                or "$" not in base_value
            ):
                return base_value
            for key in sorted(rw.environment, key=len, reverse=True):
                if "${" + key + "}" in base_value:
                    base_value = base_value.replace(
                        "${" + key + "}", str(rw.environment[key])
                    )
                # Replace longest keys first, as the following replacement is
                # not well-defined when one key is a prefix of another:
                if "$" + key in base_value:
                    base_value = base_value.replace("$" + key, str(rw.environment[key]))
            return base_value

        try:
            with self._ispyb_sessionmaker() as session:
                result = command_function(
                    rw=rw,
                    message=message,
                    parameters=parameters,
                    parameter_map=parameter_map,
                    session=session,
                    transaction=txn,
                    header=header,
                )
        except Exception as e:
            self.log.error(
                f"Uncaught exception {e!r} in ISPyB function {command!r}, "
                "quarantining message and shutting down instance.",
                exc_info=True,
            )
            rw.transport.transaction_abort(txn)
            rw.transport.nack(header)
            self._request_termination()
            return

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
                delay=rw.recipe_step["parameters"].get("delay", result.get("delay")),
                transaction=txn,
            )
            rw.transport.ack(header, transaction=txn)
        else:
            rw.transport.transaction_abort(txn)
            rw.transport.nack(header)
            return
        rw.transport.transaction_commit(txn)

    def do_create_ispyb_job(
        self, parameters, *, session: sqlalchemy.orm.session.Session, rw=None, **kwargs
    ):
        dcid = int(parameters("DCID"))
        sweeps = [(s["DCID"], s["start"], s["end"]) for s in parameters("sweeps")]
        if not dcid and not sweeps:
            self.log.error("Can not create job: neither DCID nor sweeps are specified")
            return False

        if not sweeps:
            dc = crud.get_data_collection(dcid, session)
            if not dc:
                self.log.error(f"DCID {dcid} not found")
                return False
            start = dc.startImageNumber
            number = dc.numberOfImages
            if not start or not number:
                self.log.error(
                    "Can not automatically infer data collection sweep for this DCID"
                )
                return False
            end = start + number - 1
            sweeps = [(dcid, start, end)]
            self.log.info(f"Using images {start} to {end} for data collection sweep")

        ispyb_params = [(p["key"], p["value"]) for p in parameters("parameters")]
        triggervariables = {
            p["key"]: p["value"] for p in parameters("triggervariables")
        }

        jp = self.ispyb.mx_processing.get_job_params()
        # _job_params = StrictOrderedDict([('id', None), ('datacollectionid', None), ('display_name', None), ('comments', None), ('recipe', None), ('automatic', None)])
        jp["automatic"] = parameters("source") == "automatic"
        jp["comments"] = parameters("comment")
        jp["datacollectionid"] = dcid or sweeps[0][0]
        jp["display_name"] = parameters("displayname")
        jp["recipe"] = parameters("recipe")
        self.log.info("Creating database entries...")

        jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
        self.log.info(f"  JobID={jobid}")
        for key, value in ispyb_params:
            jpp = self.ispyb.mx_processing.get_job_parameter_params()
            # _job_parameter_params = StrictOrderedDict([('id', None), ('job_id', None), ('parameter_key', None), ('parameter_value', None)])
            jpp["job_id"] = jobid
            jpp["parameter_key"] = key
            jpp["parameter_value"] = value
            jppid = self.ispyb.mx_processing.upsert_job_parameter(list(jpp.values()))
            self.log.debug(f"  JPP={jppid}")

        for sweep in sweeps:
            jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
            # _job_image_sweep_params = StrictOrderedDict([('id', None), ('job_id', None), ('datacollectionid', None), ('start_image', None), ('end_image', None)])
            jisp["job_id"] = jobid
            jisp["datacollectionid"] = sweep[0]
            jisp["start_image"] = sweep[1]
            jisp["end_image"] = sweep[2]
            jispid = self.ispyb.mx_processing.upsert_job_image_sweep(
                list(jisp.values())
            )
            self.log.debug(f"  JISP={jispid}")

        self.log.info(f"All done. Processing job {jobid} created")

        triggervariables["ispyb_process"] = jobid
        if parameters("autostart"):
            rw.send_to("trigger", {"parameters": triggervariables})
        else:
            rw.send_to("held", {"parameters": triggervariables})

        return {"success": True, "return_value": jobid}

    def do_update_processing_status(self, parameters, **kwargs):
        ppid = parameters("program_id")
        message = parameters("message")
        status = parameters("status")
        try:
            result = self.ispyb.mx_processing.upsert_program_ex(
                program_id=ppid,
                status={"success": 1, "failure": 0}.get(status),
                time_start=parameters("start_time"),
                time_update=parameters("update_time"),
                message=message,
            )
            self.log.info(
                f"Updating program {ppid} with status {message!r}",
            )
            # result is just ppid
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
            result = self.ispyb.mx_processing.upsert_run(list(params.values()))
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
        environment = parameters("environment") or ""
        if isinstance(environment, dict):
            environment = ", ".join(
                f"{key}={value}" for key, value in environment.items()
            )
        environment = environment[: min(255, len(environment))]
        rpid = parameters("rpid")
        if rpid and not rpid.isdigit():
            self.log.error("Invalid processing id '%s'", rpid)
            return False
        try:
            result = self.ispyb.mx_processing.upsert_program_ex(
                job_id=rpid,
                name=program,
                command=cmdline,
                environment=environment,
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

    def do_update_program_name(self, parameters, **kwargs):
        program_id = parameters("program_id")
        name = parameters("program")
        try:
            result = self.ispyb.mx_processing.upsert_program_ex(
                program_id=program_id,
                name=name,
            )
            self.log.info(f"Updated program name={name} for program_id={program_id}")
            return {"success": True, "return_value": result}
        except ispyb.ISPyBException as e:
            self.log.error(
                f"Updating program name={name} for program_id={program_id} caused exception {e}",
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
        params["importance_rank"] = parameters(
            "importance_rank", replace_variables=False
        )
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

    def do_add_program_message(self, parameters, **kwargs):
        result = self.ispyb.mx_processing.upsert_program_message(
            program_id=parameters("program_id"),
            severity=parameters("severity"),
            message=parameters("message"),
            description=parameters("description"),
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

    def do_insert_screening(self, parameters, **kwargs):
        """Write entry to the Screening table."""
        # screening_params: ['id', 'dcgid', 'dcid', 'programversion', 'shortcomments', 'comments']
        screening_params = self.ispyb.mx_screening.get_screening_params()
        for k in screening_params.keys():
            screening_params[k] = parameters(k)
        self.log.info("screening_params: %s", screening_params)
        try:
            screeningId = self.ispyb.mx_screening.insert_screening(
                list(screening_params.values())
            )
            assert screeningId is not None
        except (ispyb.ISPyBException, AssertionError) as e:
            self.log.error(
                "Inserting screening results: '%s' caused exception '%s'.",
                screening_params,
                e,
                exc_info=True,
            )
            return False
        self.log.info("Written Screening record with ID %s", screeningId)
        return {"success": True, "return_value": screeningId}

    def do_insert_screening_input(self, parameters, **kwargs):
        """Write entry to the ScreeningInput table."""
        # input_params: ['id', 'screening_id', 'beamx', 'beamy', 'rms_err_lim', 'min_fraction_indexed', 'max_fraction_rejected', 'min_signal2noise']
        input_params = self.ispyb.mx_screening.get_screening_input_params()
        for k in input_params.keys():
            input_params[k] = parameters(k)
        input_params["screening_id"] = parameters("screening_id")
        self.log.info("input_params: %s", input_params)
        try:
            screeningInputId = self.ispyb.mx_screening.insert_screening_input(
                list(input_params.values())
            )
            assert screeningInputId is not None
        except (ispyb.ISPyBException, AssertionError) as e:
            self.log.error(
                "Inserting screening input: '%s' caused exception '%s'.",
                input_params,
                e,
                exc_info=True,
            )
            return False
        self.log.info("Written ScreeningInput record with ID %s", screeningInputId)
        return {"success": True, "return_value": screeningInputId}

    def do_insert_screening_output(self, parameters, **kwargs):
        """Write entry to the ScreeningOutput table."""
        # output_params: ['id', 'screeningid', 'statusdescription', 'rejectedreflections', 'resolutionobtained', 'spotdeviationr', 'spotdeviationtheta', 'beamshiftx', 'beamshifty', 'numspotsfound', 'numspotsused', 'numspotsrejected', 'mosaicity', 'ioversigma', 'diffractionrings', 'mosaicityestimated', 'rankingresolution', 'program', 'dosetotal', 'totalexposuretime', 'totalrotationrange', 'totalnoimages', 'rfriedel', 'indexingsuccess', 'strategysuccess', 'alignmentsuccess']
        output_params = self.ispyb.mx_screening.get_screening_output_params()
        for k in output_params.keys():
            output_params[k] = parameters(k)
        output_params["screening_id"] = parameters("screening_id")
        output_params["alignmentSuccess"] = 1 if parameters("alignmentSuccess") else 0
        output_params["mosaicityEstimated"] = 1 if parameters("mosaicity") else 0
        output_params["indexingSuccess"] = 1
        output_params["strategySuccess"] = 1
        self.log.info("output_params: %s", output_params)
        try:
            screeningOutputId = self.ispyb.mx_screening.insert_screening_output(
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
        self.log.info("Written ScreeningOutput record with ID %s", screeningOutputId)
        return {"success": True, "return_value": screeningOutputId}

    def do_insert_screening_output_lattice(self, parameters, **kwargs):
        """Write entry to the ScreeningOutputLattice table."""
        # output_lattice_params ['id', 'screeningoutputid', 'spacegroup', 'pointgroup', 'bravaislattice', 'raworientationmatrixax', 'raworientationmatrixay', 'raworientationmatrixaz', 'raworientationmatrixbx', 'raworientationmatrixby', 'raworientationmatrixbz', 'raworientationmatrixcx', 'raworientationmatrixcy', 'raworientationmatrixcz', 'unitcella', 'unitcellb', 'unitcellc', 'unitcellalpha', 'unitcellbeta', 'unitcellgamma', 'labelitindexing']
        output_lattice_params = (
            self.ispyb.mx_screening.get_screening_output_lattice_params()
        )
        for k in output_lattice_params.keys():
            output_lattice_params[k] = parameters(k)
        output_lattice_params["screening_output_id"] = parameters("screening_output_id")
        self.log.info("output_lattice_params: %s", output_lattice_params)
        try:
            screeningOutputLatticeId = (
                self.ispyb.mx_screening.insert_screening_output_lattice(
                    list(output_lattice_params.values())
                )
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
        return {"success": True, "return_value": screeningOutputLatticeId}

    def do_insert_screening_strategy(self, parameters, **kwargs):
        """Write entry to the ScreeningStrategy table."""
        # strategy_params ['id', 'screeningoutputid', 'phistart', 'phiend', 'rotation', 'exposuretime', 'resolution', 'completeness', 'multiplicity', 'anomalous', 'program', 'rankingresolution', 'transmission']
        strategy_params = self.ispyb.mx_screening.get_screening_strategy_params()
        for k in strategy_params.keys():
            strategy_params[k] = parameters(k)
        strategy_params["screening_output_id"] = parameters("screening_output_id")
        strategy_params["anomalous"] = parameters("anomalous") or 0
        self.log.info("strategy_params: %s", strategy_params)
        try:
            screeningStrategyId = self.ispyb.mx_screening.insert_screening_strategy(
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
        return {"success": True, "return_value": screeningStrategyId}

    def do_insert_screening_strategy_wedge(self, parameters, **kwargs):
        """Write entry to the ScreeningStrategyWedge table."""
        # wedge_params ['id', 'screeningstrategyid', 'wedgenumber', 'resolution', 'completeness', 'multiplicity', 'dosetotal', 'noimages', 'phi', 'kappa', 'chi', 'comments', 'wavelength']
        wedge_params = self.ispyb.mx_screening.get_screening_strategy_wedge_params()
        for k in wedge_params.keys():
            wedge_params[k] = parameters(k)
        wedge_params["screening_strategy_id"] = parameters("screening_strategy_id")
        wedge_params["wedgenumber"] = parameters("wedgenumber") or "1"
        self.log.info("wedge_params: %s", wedge_params)
        try:
            screeningStrategyWedgeId = (
                self.ispyb.mx_screening.insert_screening_strategy_wedge(
                    list(wedge_params.values())
                )
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
        return {"success": True, "return_value": screeningStrategyWedgeId}

    def do_insert_screening_strategy_sub_wedge(self, parameters, **kwargs):
        """Write entry to the ScreeningStrategySubWedge table."""
        # sub_wedge_params ['id', 'screeningstrategywedgeid', 'subwedgenumber', 'rotationaxis', 'axisstart', 'axisend', 'exposuretime', 'transmission', 'oscillationrange', 'completeness', 'multiplicity', 'resolution', 'dosetotal', 'noimages', 'comments']
        sub_wedge_params = (
            self.ispyb.mx_screening.get_screening_strategy_sub_wedge_params()
        )
        for k in sub_wedge_params.keys():
            sub_wedge_params[k] = parameters(k)
        sub_wedge_params["screening_strategy_wedge_id"] = parameters(
            "screening_strategy_wedge_id"
        )
        sub_wedge_params["subwedgenumber"] = "1"
        self.log.info("sub_wedge_params: %s", sub_wedge_params)
        try:
            screeningStrategySubWedgeId = (
                self.ispyb.mx_screening.insert_screening_strategy_sub_wedge(
                    list(sub_wedge_params.values())
                )
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
        return {"success": True, "return_value": screeningStrategySubWedgeId}

    def do_register_integration(self, **kwargs):
        # deprecated
        self.log.warning(
            "Call to deprecated register_integration function (use upsert_integration instead)"
        )
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
                "res_i_sig_i_2",
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

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def do_insert_dimple_result(
        self,
        *,
        parameter_map: DimpleResult,
        session: sqlalchemy.orm.session.Session,
        **kwargs,
    ):
        mxmrrun = crud.insert_dimple_result(
            mxmrrun=parameter_map.mxmrrun,
            blobs=parameter_map.blobs,
            auto_proc_program=parameter_map.auto_proc_program,
            attachments=parameter_map.attachments,
            session=session,
        )
        return {"success": True, "return_value": mxmrrun.mxMRRunId}

    def do_insert_mxmr_run(self, parameters, **kwargs):
        params = self.ispyb.mx_processing.get_run_params()
        for k in params.keys():
            if parameters(k) is not None:
                params[k] = parameters(k)
        params["parentid"] = parameters("scaling_id")
        self.log.debug(params)
        mxmr_run_id = self.ispyb.mx_processing.upsert_run(list(params.values()))
        self.log.info("Written MXMRRun record with ID %s", mxmr_run_id)
        return {"success": True, "return_value": mxmr_run_id}

    def do_insert_mxmr_run_blob(self, parameters, **kwargs):
        params = self.ispyb.mx_processing.get_run_blob_params()
        for k in params.keys():
            if parameters(k) is not None:
                params[k] = parameters(k)
        params["parentid"] = parameters("mxmr_run_id")
        self.log.debug(params)
        mxmr_run_blob_id = self.ispyb.mx_processing.upsert_run_blob(
            list(params.values())
        )
        self.log.info("Written MXMRRunBlob record with ID %s", mxmr_run_blob_id)
        return {"success": True, "return_value": mxmr_run_blob_id}

    def do_retrieve_programs_for_job_id(self, parameters, **kwargs):
        """Retrieve the processing instances associated with the given processing job ID"""

        processingJobId = parameters("rpid")
        result = self.ispyb.mx_processing.retrieve_programs_for_job_id(processingJobId)
        serial_result = []
        for row in result:
            el = {}
            for k, v in row.items():
                try:
                    json.dumps(v)
                    el[k] = v
                except TypeError:
                    continue
            serial_result.append(el)
        return {"success": True, "return_value": serial_result}

    def do_retrieve_program_attachments_for_program_id(self, parameters, **kwargs):
        """Retrieve the processing program attachments associated with the given processing program ID"""

        autoProcProgramId = parameters("program_id")
        result = self.ispyb.mx_processing.retrieve_program_attachments_for_program_id(
            autoProcProgramId
        )
        return {"success": True, "return_value": result}

    def do_retrieve_proposal_title(self, parameters, **kwargs):
        """Get the title of a given proposal"""

        try:
            visit = parameters("visit")
            proposal, _ = visit.split("-")
            proposal_code, proposal_number = proposal[:2], proposal[2:]
        except (AttributeError, ValueError):
            proposal_code = parameters("proposalcode")
            proposal_number = parameters("proposalnumber")

        result = self.ispyb.core.retrieve_proposal_title(proposal_code, proposal_number)
        try:
            title = result[0]["title"]
            return {"success": True, "return_value": title}
        except (IndexError, KeyError):
            if visit is None:
                self.log.error(
                    "Cannot find proposal title record for proposalcode %s "
                    "and proposalnumber %s",
                    proposal_code,
                    proposal_number,
                    exc_info=True,
                )
            else:
                self.log.error(
                    "Cannot find proposal title record for visit %s",
                    visit,
                    exc_info=True,
                )
            return False

    def do_insert_phasing_analysis_results(self, parameters, **kwargs):
        """Write phasing results to ISPyB"""

        phasing_results = parameters("phasing_results")
        if not phasing_results:
            self.log.error(
                "No phasing results found in message",
                exc_info=True,
            )
            return False

        scaling_id = parameters("scaling_id")
        if not scaling_id:
            self.log.error(
                "No scaling_id found in message",
                exc_info=True,
            )
            return False

        self.log.debug(
            f"Inserting phasing results for scaling_id={scaling_id}\n"
            f"{phasing_results}"
        )

        try:
            phasing_id = self.ispyb.mx_processing.insert_phasing_analysis_results(
                phasing_results, scaling_id
            )
            assert phasing_id and phasing_id > 0
            self.log.debug(f"Written phasing results with phasing_id={phasing_id}")
            return {"success": True, "return_value": phasing_id}
        except (ispyb.ISPyBException, AssertionError) as e:
            self.log.error(
                f"Error inserting phasing results for scaling_id={scaling_id}:\n"
                f"{phasing_results} caused exception {e}",
                exc_info=True,
            )
            return False

    def do_insert_pdb_files(self, *, parameters, session, **kwargs):
        protein_id = parameters("protein_id")
        if not protein_id:
            self.log.error(
                "No protein_id found in message",
                exc_info=True,
            )

        source = parameters("source")
        pdb_files = [pathlib.Path(p) for p in parameters("pdb_files")]
        if not pdb_files:
            self.log.error(
                "No pdb_files found in message",
                exc_info=True,
            )

        for pdb_file in pdb_files:
            if not pdb_file.is_file():
                self.log.warning(
                    f"PDB file {pdb_file} does not appear to exist",
                    exc_info=True,
                )
                continue

            pdb = PDB(name=pdb_file.name, contents=pdb_file.read_text(), source=source)
            protein_has_pdb = ProteinHasPDB(proteinid=protein_id, PDB=pdb)
            self.log.debug(f"Inserting PDB {pdb.name} for proteinId {protein_id}")

            session.add_all([pdb, protein_has_pdb])

        session.commit()
        return {"success": True}

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
        # if not isinstance(message, dict) and message:
        #     self.log.warning(
        #         "Multipart messages with non-dictionary messages are deprecated"
        #     )
        # Disabling this warning 2021-07-22, as this does occur, even though the messages
        # in that case are discarded as far as I can tell.

        step = 1
        commands = rw.recipe_step["parameters"].get("ispyb_command_list")
        if isinstance(message, dict) and isinstance(
            message.get("ispyb_command_list"), list
        ):
            commands = message["ispyb_command_list"]
            step = message.get("checkpoint", 0) + 1
        if not commands:
            self.log.error("Received multipart message containing no commands")
            return False

        current_command = commands[0]
        command = current_command.get("ispyb_command")
        if not command:
            self.log.error(
                "Multipart command %s is not a valid ISPyB command", current_command
            )
            return False
        command_function = lookup_command(command, self)
        if not command_function:
            self.log.error("Received unknown ISPyB command (%s)", command)
            return False
        self.log.debug(
            "Processing step %d of multipart message (%s) with %d further steps",
            step,
            command,
            len(commands) - 1,
            extra={"ispyb-message-parts": len(commands)} if step == 1 else {},
        )

        # Create a parameter lookup function specific to this step of the
        # multipart message
        def step_parameters(parameter, replace_variables=True):
            """Slight change in behaviour compared to 'parameters' in a direct call:
            If the value is defined in the command list item then this takes
            precedence. Otherwise we check the original message content. Finally,
            we look in the parameters dictionary of the recipe step for the
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
                or not isinstance(base_value, str)
                or "$" not in base_value
            ):
                return base_value
            for key in sorted(rw.environment, key=len, reverse=True):
                if "${" + key + "}" in base_value:
                    base_value = base_value.replace(
                        "${" + key + "}", str(rw.environment[key])
                    )
                # Replace longest keys first, as the following replacement is
                # not well-defined when one key is a prefix of another:
                if "$" + key in base_value:
                    base_value = base_value.replace("$" + key, str(rw.environment[key]))
            return base_value

        kwargs["parameters"] = step_parameters
        kwargs["parameter_map"] = ChainMapWithReplacement(
            current_command,
            message if isinstance(message, dict) else {},
            rw.recipe_step["parameters"],
            substitutions=rw.environment,
        )

        # If this step previously checkpointed then override the message passed
        # to the step.
        step_message = current_command
        if isinstance(message, dict):
            step_message = message.get("step_message", step_message)

        # Run the multipart step
        result = command_function(rw=rw, message=step_message, **kwargs)

        # Store step result if appropriate
        store_result = current_command.get("store_result")
        if store_result and result and "return_value" in result:
            rw.environment[store_result] = result["return_value"]
            self.log.debug(
                "Storing result '%s' in environment variable '%s'",
                result["return_value"],
                store_result,
            )

        # If the current step has checkpointed then need to manage this
        if result and result.get("checkpoint"):
            self.log.debug("Checkpointing for sub-command %s", command)

            if isinstance(message, dict):
                checkpoint_dictionary = message
            else:
                checkpoint_dictionary = {}
            checkpoint_dictionary["checkpoint"] = step - 1
            checkpoint_dictionary["ispyb_command_list"] = commands
            checkpoint_dictionary["step_message"] = result.get("return_value")
            return {
                "checkpoint": True,
                "return_value": checkpoint_dictionary,
                "delay": result.get("delay"),
            }

        # If the step did not succeed then propagate failure
        if not result or not result.get("success"):
            self.log.debug("Multipart command failed")
            return result

        # Step has completed, so remove from queue
        commands.pop(0)

        # If the multipart command is finished then propagate success
        if not commands:
            self.log.debug("and done.")
            return result

        # If there are more steps then checkpoint the current state
        # and put it back on the queue (with no delay)
        self.log.debug("Checkpointing remaining %d steps", len(commands))
        if isinstance(message, dict):
            checkpoint_dictionary = message
        else:
            checkpoint_dictionary = {}
        checkpoint_dictionary["checkpoint"] = step
        checkpoint_dictionary["ispyb_command_list"] = commands
        if "step_message" in checkpoint_dictionary:
            del checkpoint_dictionary["step_message"]
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

    def do_buffer(self, rw, message, session, parameters, header, **kwargs):
        """The buffer command supports running buffer lookups before running
        a command, and optionally storing the result in a buffer after running
        the command. It also takes care of checkpointing in case a required
        buffer value is not yet available.

        As an example, if you want to send this message to the ISPyB service:

        {
            "ispyb_command": "insert_thing",
            "parent_id": "$ispyb_thing_parent_id",
            "store_result": "ispyb_thing_id",
            "parameter_a": ...,
            "parameter_b": ...,
        }

        and want to look up the parent_id using the buffer with your unique
        reference UUID1 you could write:

        {
            "ispyb_command": "buffer",
            "program_id": "$ispyb_autoprocprogram_id",
            "buffer_lookup": {
                "parent_id": UUID1,
            },
            "buffer_command": {
                "ispyb_command": "insert_thing",
                "parameter_a": ...,
                "parameter_b": ...,
            },
            "buffer_store": UUID2,
            "store_result": "ispyb_thing_id",
        }

        which would also store the result under buffer reference UUID2.
        """

        if not rw.environment.get("has_recipe_wrapper", True):
            self.log.error("Buffer call can not be used with simple messages")
            return False

        if not isinstance(message, dict):
            self.log.error("Invalid buffer call: message must be a dictionary")
            return False

        if not isinstance(message.get("buffer_command"), dict) or not message[
            "buffer_command"
        ].get("ispyb_command"):
            self.log.error("Invalid buffer call: no buffer command specified")
            return False

        command_function = lookup_command(
            message["buffer_command"]["ispyb_command"], self
        )
        if not command_function:
            self.log.error("Invalid buffer call: unknown command specified")
            return False

        if ("buffer_expiry_time" not in message) or (
            header.get("dlq-reinjected") in {True, "True", "true", 1}
        ):
            message["buffer_expiry_time"] = time.time() + 600

        # Prepare command: Resolve all references
        program_id = parameters("program_id")
        if message.get("buffer_lookup"):
            if not isinstance(message["buffer_lookup"], dict):
                self.log.error(
                    "Invalid buffer call: buffer_lookup dictionary is not a dictionary"
                )
                return False
            if not program_id:
                self.log.error("Invalid buffer call: program_id is undefined")
                return False
            for entry in list(message["buffer_lookup"]):
                buffer_result = buffer.load(
                    session=session,
                    program=program_id,
                    uuid=message["buffer_lookup"][entry],
                )
                if buffer_result.success:
                    # resolve value and continue
                    message["buffer_command"][entry] = buffer_result.value
                    del message["buffer_lookup"][entry]
                    self.log.debug(
                        f"Successfully resolved buffer reference {entry!r} to {buffer_result.value!r}"
                    )
                    continue

                if message["buffer_expiry_time"] < time.time():
                    self.log.warning(
                        f"Buffer call could not be resolved: entry {entry} not found for program {program_id}"
                    )
                    return False

                # value can not yet be resolved, put request back in the queue
                return {"checkpoint": True, "return_value": message, "delay": 20}

        # Run the actual command
        result = command_function(
            rw=rw,
            message=message["buffer_command"],
            session=session,
            parameters=parameters,
            **kwargs,
        )

        # Store result if appropriate
        store_result = message.get("store_result")
        if store_result and result and "return_value" in result:
            rw.environment[store_result] = result["return_value"]
            self.log.debug(
                "Storing result '%s' in environment variable '%s'",
                result["return_value"],
                store_result,
            )

        # If the actual command has checkpointed then need to manage this
        if result and result.get("checkpoint"):
            self.log.debug("Checkpointing for buffered function")
            message["buffer_command"] = result["return_value"]
            return {
                "checkpoint": True,
                "return_value": message,
                "delay": result.get("delay"),
            }

        # If the command did not succeed then propagate failure
        if not result or not result.get("success"):
            self.log.warning("Buffered command failed")
            # should become debug level eventually, the actual function will do the warning
            return result

        # Optionally store a reference to the result in the buffer table
        if message.get("buffer_store"):
            self.log.debug("Storing buffer result for UUID %r", message["buffer_store"])
            buffer.store(
                session=session,
                program=program_id,
                uuid=message["buffer_store"],
                reference=result["return_value"],
            )

        # Finally, propagate result
        return result

    def do_insert_data_collection_group(self, parameters, message=None, **kwargs):
        dcgparams = self.ispyb.em_acquisition.get_data_collection_group_params()
        dcgparams["parentid"] = parameters("session_id")
        dcgparams["experimenttype"] = parameters("experiment_type")
        dcgparams["starttime"] = parameters("start_time")
        dcgparams["comments"] = "Created for Murfey"
        try:
            data_collection_group_id = (
                self.ispyb.em_acquisition.upsert_data_collection_group(
                    list(dcgparams.values())
                )
            )
            self.log.info(f"Created DataCollectionGroup {data_collection_group_id}")
            return {"success": True, "return_value": data_collection_group_id}

        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting Data Collection Group entry caused exception '%s'.",
                e,
                exc_info=True,
            )
        return False

    def do_insert_data_collection(self, parameters, message=None, **kwargs):
        dc_params = self.ispyb.em_acquisition.get_data_collection_params()
        dc_params["parentid"] = parameters("dcgid")
        dc_params["starttime"] = parameters("start_time")
        dc_params["imgdir"] = parameters("image_directory")
        dc_params["imgsuffix"] = parameters("image_suffix")
        dc_params["visitid"] = parameters("session_id")
        dc_params["voltage"] = parameters("voltage")
        try:
            data_collection_id = self.ispyb.em_acquisition.upsert_data_collection(
                list(dc_params.values())
            )
            self.log.info(f"Created DataCollection {data_collection_id}")
            return {"success": True, "return_value": data_collection_id}

        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting Data Collection entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return False

    def do_upsert_data_collection_mx(self, parameters, message=None, **kwargs):
        dc_params = self.ispyb.mx_acquisition.get_data_collection_params()
        dc_params["id"] = parameters("dcid")
        dc_params["parentid"] = parameters("dcgid")
        dc_params["xtal_snapshot1"] = parameters("xtal_snapshot1")
        dc_params["xtal_snapshot2"] = parameters("xtal_snapshot2")
        dc_params["xtal_snapshot3"] = parameters("xtal_snapshot3")
        dc_params["xtal_snapshot4"] = parameters("xtal_snapshot4")
        try:
            data_collection_id = self.ispyb.mx_acquisition.upsert_data_collection(
                list(dc_params.values())
            )
            self.log.info(f"Upserted DataCollection {data_collection_id}")
            return {"success": True, "return_value": data_collection_id}

        except ispyb.ISPyBException as e:
            self.log.error(
                f"Upserting DataCollection entry caused exception '{e}'.",
                exc_info=True,
            )
            return False
