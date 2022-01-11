from __future__ import annotations

import json
import logging
import subprocess

import procrunner
import py
import zocalo.wrapper

import dlstbx.util.symlink
from dlstbx.util.merging_statistics import get_merging_statistics

logger = logging.getLogger("dlstbx.wrap.fast_dp")


class FastDPWrapper(zocalo.wrapper.BaseWrapper):
    def send_results_to_ispyb(self, z, xtriage_results=None):
        ispyb_command_list = []

        # Step 1: Add new record to AutoProc, keep the AutoProcID
        register_autoproc = {
            "ispyb_command": "write_autoproc",
            "autoproc_id": None,
            "store_result": "ispyb_autoproc_id",
            "spacegroup": z["spacegroup"],
            "refinedcell_a": z["unit_cell"][0],
            "refinedcell_b": z["unit_cell"][1],
            "refinedcell_c": z["unit_cell"][2],
            "refinedcell_alpha": z["unit_cell"][3],
            "refinedcell_beta": z["unit_cell"][4],
            "refinedcell_gamma": z["unit_cell"][5],
        }
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

        # Step 3: Store integration result, linked to the ScalingID
        integration = {
            "ispyb_command": "upsert_integration",
            "scaling_id": "$ispyb_autoprocscaling_id",
            "cell_a": z["unit_cell"][0],
            "cell_b": z["unit_cell"][1],
            "cell_c": z["unit_cell"][2],
            "cell_alpha": z["unit_cell"][3],
            "cell_beta": z["unit_cell"][4],
            "cell_gamma": z["unit_cell"][5],
            "refined_xbeam": z["refined_beam"][0],
            "refined_ybeam": z["refined_beam"][1],
        }
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

    def construct_commandline(self, params):
        """Construct fast_dp command line.
        Takes job parameter dictionary, returns array."""

        command = [
            "fast_dp",
            "--atom=S",
            "-j",
            "0",
            "-J",
            "18",
            "-l",
            "durin-plugin.so",
            params["fast_dp"]["filename"],
        ]

        if params.get("ispyb_parameters"):
            if params["ispyb_parameters"].get("d_min"):
                command.append(
                    "--resolution-high=%s" % params["ispyb_parameters"]["d_min"]
                )
            if params["ispyb_parameters"].get("spacegroup"):
                command.append(
                    "--spacegroup=%s" % params["ispyb_parameters"]["spacegroup"]
                )
            if params["ispyb_parameters"].get("unit_cell"):
                command.append("--cell=%s" % params["ispyb_parameters"]["unit_cell"])

        return command

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        command = self.construct_commandline(params)

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])

        # Create working directory with symbolic link
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory.strpath, params["create_symlink"]
            )

        # Create SynchWeb ticks hack file. This will be overwritten with the real log later.
        # For this we need to create the results directory and symlink immediately.
        if params.get("synchweb_ticks"):
            logger.debug("Setting SynchWeb status to swirl")
            if params.get("create_symlink"):
                results_directory.ensure(dir=True)
                dlstbx.util.symlink.create_parent_symlink(
                    results_directory.strpath, params["create_symlink"]
                )
            py.path.local(params["synchweb_ticks"]).ensure()

        # Set appropriate environment variables for forkxds
        environment = {}
        if params.get("forkxds_queue"):
            environment["FORKXDS_QUEUE"] = params["forkxds_queue"]
        if params.get("forkxds_project"):
            environment["FORKXDS_PROJECT"] = params["forkxds_project"]

        # run fast_dp in working directory
        logger.info("command: %s", " ".join(command))
        try:
            result = procrunner.run(
                command,
                timeout=params.get("timeout"),
                working_directory=working_directory,
                environment_override=environment,
                raise_timeout_exception=True,
            )
            success = not result.returncode
            if success:
                logger.info("fast_dp successful")
            else:
                logger.info(f"fast_dp failed with exitcode {result.returncode}")
                logger.debug(result.stdout)
                logger.debug(result.stderr)
        except subprocess.TimeoutExpired as te:
            logger.info("fast_dp failed with timeout")
            logger.debug(te.stdout)
            logger.debug(te.stderr)
            success = False

        if success and working_directory.join("fast_dp.error").check():
            # fast_dp anomaly: exit code 0 and no stderr output still means failure if error file exists
            success = False
            logger.warning("fast_dp exited with error, but with returncode 0")
            logger.debug(result.stdout)
            logger.debug(result.stderr)

        if success:
            command = [
                "xia2.report",
                "log_include=%s" % working_directory.join("fast_dp.log").strpath,
                "prefix=fast_dp",
                "title=fast_dp",
                "fast_dp_unmerged.mtz",
            ]
            # run xia2.report in working directory
            logger.info("Running command: %s", " ".join(command))
            try:
                result = procrunner.run(
                    command,
                    timeout=params.get("timeout"),
                    raise_timeout_exception=True,
                    working_directory=working_directory,
                )
            except subprocess.TimeoutExpired as te:
                success = False
                logger.warning(f"xia2.report timed out: {te.timeout}\n  {te.cmd}")
                logger.debug(te.stdout)
                logger.debug(te.stderr)
            else:
                success = not result.returncode
                if success:
                    logger.info("xia2.report successful")
                else:
                    logger.info(f"xia2.report failed with exitcode {result.returncode}")
                    logger.debug(result.stdout)
                    logger.debug(result.stderr)

            json_file = working_directory.join("iotbx-merging-stats.json")
            with json_file.open("w") as fh:
                fh.write(
                    get_merging_statistics(
                        str(working_directory.join("fast_dp_unmerged.mtz").strpath)
                    ).as_json()
                )

        # Create results directory and symlink if they don't already exist
        results_directory.ensure(dir=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                results_directory.strpath, params["create_symlink"]
            )

        # copy output files to result directory and attach them in ISPyB
        keep_ext = {
            ".cbf": False,
            ".INP": False,
            ".xml": False,
            ".state": False,
            ".log": "log",
            ".html": "log",
            ".txt": "log",
            ".error": "log",
            ".LP": "log",
            ".HKL": "result",
            ".sca": "result",
            ".mtz": "result",
        }
        keep = {"iotbx-merging-stats.json": "graph", "fast_dp-report.json": "result"}

        # Record these log files first so they appear at the top of the list
        # of attachments in SynchWeb
        primary_log_files = [
            working_directory.join("fast_dp-report.html"),
            working_directory.join("fast_dp.log"),
        ]

        allfiles = []
        for filename in primary_log_files + working_directory.listdir():
            if not filename.check():
                continue
            filetype = keep_ext.get(filename.ext)
            if filename.basename in keep:
                filetype = keep[filename.basename]
            if filetype is None:
                continue
            destination = results_directory.join(filename.basename)
            if destination.strpath in allfiles:
                # We've already seen this file above
                continue
            logger.debug(f"Copying {filename.strpath} to {destination.strpath}")
            allfiles.append(destination.strpath)
            filename.copy(destination)
            if filetype:
                self.record_result_individual_file(
                    {
                        "file_path": destination.dirname,
                        "file_name": destination.basename,
                        "file_type": filetype,
                        "importance_rank": 1
                        if destination.basename
                        in ("fast_dp.mtz", "fast_dp-report.html")
                        else 2,
                    }
                )
        if allfiles:
            self.record_result_all_files({"filelist": allfiles})

        # Forward JSON results if possible
        if success and working_directory.join("fast_dp.json").check():
            with working_directory.join("fast_dp.json").open("r") as fh:
                json_data = json.load(fh)
            if (
                params.get("store_xtriage_results")
                and working_directory.join("fast_dp-report.json").check()
            ):
                with working_directory.join("fast_dp-report.json").open("rb") as fh:
                    xtriage_results = json.load(fh).get("xtriage")
            else:
                xtriage_results = None
            self.send_results_to_ispyb(json_data, xtriage_results=xtriage_results)
        elif success:
            logger.warning("Expected JSON output file missing")

        return success
