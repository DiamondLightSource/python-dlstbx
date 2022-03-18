from __future__ import annotations

import datetime
import json
import logging
import os
import subprocess

import dateutil.parser
import procrunner
import py

import dlstbx.util.symlink
from dlstbx.wrapper import Wrapper

logger = logging.getLogger("dlstbx.wrap.xia2")


class Xia2Wrapper(Wrapper):
    def construct_commandline(self, params):
        """Construct xia2 command line.
        Takes job parameter dictionary, returns array."""

        command = ["xia2"]

        for param, values in params["xia2"].items():
            if param == "images":
                if not values:
                    # This may be empty if related data collections are requested, but no related DCs were found
                    continue
                param = "image"
                values = values.split(",")
            if not isinstance(values, (list, tuple)):
                values = [values]
            for v in values:
                command.append(f"{param}={v}")

        if params.get("ispyb_parameters"):
            translation = {
                "d_min": "xia2.settings.resolution.d_min",
                "spacegroup": "xia2.settings.space_group",
                "unit_cell": "xia2.settings.unit_cell",
            }
            for param, value in params["ispyb_parameters"].items():
                command.append(translation.get(param, param) + "=" + value)

        return command

    def send_results_to_ispyb(self, xtriage_results=None):
        logger.info("Reading xia2 results")
        from xia2.cli.ispyb_json import zocalo_object

        z = zocalo_object()

        ispyb_command_list = []

        # Step 1: Add new record to AutoProc, keep the AutoProcID
        register_autoproc = z["refined_results"]
        register_autoproc.update(
            {
                "ispyb_command": "write_autoproc",
                "autoproc_id": None,
                "store_result": "ispyb_autoproc_id",
            }
        )
        ispyb_command_list.append(register_autoproc)

        # Step 2: Store scaling results, linked to the AutoProcID
        #         Keep the AutoProcScalingID
        insert_scaling = z["scaling_statistics"]
        insert_scaling.update(
            {
                "ispyb_command": "insert_scaling",
                "autoproc_id": "$ispyb_autoproc_id",
                "store_result": "ispyb_autoprocscaling_id",
            }
        )
        ispyb_command_list.append(insert_scaling)

        # Step 3: Store integration results, linking them to ScalingID
        for n, integration in enumerate(z["integrations"]):
            integration.update(
                {
                    "ispyb_command": "upsert_integration",
                    "scaling_id": "$ispyb_autoprocscaling_id",
                }
            )
            if n > 0:
                # make sure only the first integration uses a specified integration ID
                # and all subsequent integration results are written to a new record
                integration["integration_id"] = None
            ispyb_command_list.append(integration)

        if xtriage_results is not None:
            for d in xtriage_results:
                if (
                    d["text"]
                    == "The merging statistics indicate that the data may be assigned to the wrong space group."
                ):
                    # this is not a useful warning
                    continue
                ispyb_command_list.append(
                    {
                        "ispyb_command": "add_program_message",
                        "program_id": "$ispyb_autoprocprogram_id",
                        "message": d["text"],
                        "description": d["summary"],
                        "severity": {0: "INFO", 1: "WARNING", 2: "ERROR"}.get(
                            d["level"]
                        ),
                    }
                )

        logger.info("Sending %s", str(ispyb_command_list))
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        logger.info("Sent %d commands to ISPyB", len(ispyb_command_list))

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        command = self.construct_commandline(params)

        # Adjust all paths if a spacegroup is set in ISPyB
        if params.get("ispyb_parameters"):
            if (
                params["ispyb_parameters"].get("spacegroup")
                and "/" not in params["ispyb_parameters"]["spacegroup"]
            ):
                if "create_symlink" in params:
                    params["create_symlink"] += (
                        "-" + params["ispyb_parameters"]["spacegroup"]
                    )

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])

        # Create working directory with symbolic link
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory.strpath, params["create_symlink"]
            )

        logger.info("command: %s", " ".join(command))
        try:
            result = procrunner.run(
                command,
                timeout=params.get("timeout"),
                raise_timeout_exception=True,
                working_directory=working_directory.strpath,
            )
        except subprocess.TimeoutExpired as te:
            success = False
            logger.warning(f"xia2 timed out: {te.timeout}\n  {te.cmd}")
            logger.debug(te.stdout)
            logger.debug(te.stderr)
        else:
            success = not result.returncode
            if success:
                logger.info("xia2 successful")
            else:
                logger.info(f"xia2 failed with exitcode {result.returncode}")
                logger.debug(result.stdout)
                logger.debug(result.stderr)

        # copy output files to result directory
        results_directory.ensure(dir=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                results_directory.strpath, params["create_symlink"]
            )

        for subdir in ("DataFiles", "LogFiles"):
            src = working_directory.join(subdir)
            dst = results_directory.join(subdir)
            if src.check():
                logger.debug(f"Recursively copying {src.strpath} to {dst.strpath}")
                src.copy(dst)
            elif not success:
                logger.info(
                    f"Expected output directory does not exist (non-zero exitcode): {src.strpath}"
                )
            else:
                logger.warning(
                    f"Expected output directory does not exist: {src.strpath}"
                )

        allfiles = []
        for f in working_directory.listdir("*.*"):
            if f.check(file=1, exists=1) and not f.basename.startswith("."):
                logger.debug(f"Copying {f.strpath} to results directory")
                f.copy(results_directory)
                allfiles.append(results_directory.join(f.basename))

        # Send results to various listeners
        logfiles = ("xia2.html", "xia2.txt", "xia2.error", "xia2-error.txt")
        for result_file in map(results_directory.join, logfiles):
            if result_file.check(file=1):
                self.record_result_individual_file(
                    {
                        "file_path": result_file.dirname,
                        "file_name": result_file.basename,
                        "file_type": "log",
                        "importance_rank": 1
                        if result_file.basename == "xia2.html"
                        else 2,
                    }
                )

        datafiles_path = results_directory.join("DataFiles")
        if datafiles_path.check():
            for result_file in datafiles_path.listdir(fil=os.path.isfile):
                file_type = "result"
                if result_file.ext in (".log", ".txt"):
                    file_type = "log"
                self.record_result_individual_file(
                    {
                        "file_path": result_file.dirname,
                        "file_name": result_file.basename,
                        "file_type": file_type,
                        "importance_rank": 1
                        if result_file.basename.endswith("_free.mtz")
                        else 2,
                    }
                )
                allfiles.append(result_file.strpath)

        logfiles_path = results_directory.join("LogFiles")
        if logfiles_path.check():
            for result_file in logfiles_path.listdir(fil=os.path.isfile):
                file_type = "log"
                if result_file.ext == ".json":
                    file_type = "graph"
                elif result_file.ext == ".png":
                    file_type = "log"
                self.record_result_individual_file(
                    {
                        "file_path": result_file.dirname,
                        "file_name": result_file.basename,
                        "file_type": file_type,
                        "importance_rank": 2,
                    }
                )
                allfiles.append(result_file.strpath)

        # Part of the result parsing requires to be in result directory
        with results_directory.as_cwd():
            if params.get("store_xtriage_results") and os.path.isfile(
                "xia2-report.json"
            ):
                with open("xia2-report.json") as fh:
                    xtriage_results = json.load(fh).get("xtriage")
            else:
                xtriage_results = None
            if (
                success
                and not (
                    os.path.isfile("xia2-error.txt") or os.path.isfile("xia2.error")
                )
                and os.path.exists("xia2.json")
                and not params.get("do_not_write_to_ispyb")
            ):
                self.send_results_to_ispyb(xtriage_results=xtriage_results)

        if allfiles:
            self.record_result_all_files({"filelist": allfiles})

        if dc_end_time := params.get("dc_end_time"):
            dc_end_time = dateutil.parser.parse(dc_end_time)
            pipeline = params["xia2"].get("pipeline", "")
            dcid = params.get("dcid")
            latency_s = (datetime.datetime.now() - dc_end_time).total_seconds()
            program_name = f"xia2-{pipeline}" if pipeline else "xia2"
            logger.info(
                f"{program_name} completed for DCID {dcid} with latency of {latency_s:.2f} seconds",
                extra={f"{program_name}-latency-seconds": latency_s},
            )

        return success
