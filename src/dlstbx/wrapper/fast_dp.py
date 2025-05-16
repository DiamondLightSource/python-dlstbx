from __future__ import annotations

import datetime
import json
import os
import pathlib
import shutil
import subprocess
import time
from fnmatch import fnmatch

import dateutil.parser
from prometheus_client import Histogram

import dlstbx.util.symlink
import dlstbx.wrapper
from dlstbx.util.merging_statistics import get_merging_statistics


class FastDPWrapper(dlstbx.wrapper.Wrapper):
    _logger_name = "dlstbx.wrap.fast_dp"

    name = "fast_dp"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._latency_hist = Histogram(
            "zocalo_wrap_latency_seconds",
            "Time from end of data collection to completion of job (seconds)",
            registry=self._registry,
            buckets=dlstbx.wrapper.HISTOGRAM_BUCKETS,
        )

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
                if d["text"] in {
                    "The merging statistics indicate that the data may be assigned to the wrong space group.",
                    "The resolution of the data may be useful to higher resolution than the given resolution.",
                }:
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

        self.log.info("Sending %s", str(ispyb_command_list))
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        self.log.info("Sent %d commands to ISPyB", len(ispyb_command_list))

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

        working_directory = pathlib.Path(params["working_directory"])
        results_directory = pathlib.Path(params["results_directory"])

        # Create working directory with symbolic link
        working_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory, params["create_symlink"]
            )

        # Set appropriate environment variables for forkxds
        environment = dict(os.environ)
        if params.get("forkxds_queue"):
            environment["FORKXDS_QUEUE"] = params["forkxds_queue"]
        if params.get("forkxds_project"):
            environment["FORKXDS_PROJECT"] = params["forkxds_project"]

        # run fast_dp in working directory
        self.log.info("command: %s", " ".join(command))
        try:
            start_time = time.perf_counter()
            result = subprocess.run(
                command,
                timeout=params.get("timeout"),
                env=environment,
                cwd=working_directory,
            )
            runtime = time.perf_counter() - start_time
            self.log.info(f"fast_dp took {runtime} seconds")
            self._runtime_hist.observe(runtime)
        except subprocess.TimeoutExpired as te:
            success = False
            self.log.warning(f"fast_dp timed out: {te.timeout}\n  {te.cmd}")
            self.log.debug(te.stdout)
            self.log.debug(te.stderr)
            self._timeout_counter.inc()
        else:
            success = not result.returncode
            if success:
                self.log.info("fast_dp successful")
            else:
                self.log.info(f"fast_dp failed with exitcode {result.returncode}")
                self.log.debug(result.stdout)
                self.log.debug(result.stderr)

        if success and (working_directory / "fast_dp.error").is_file():
            # fast_dp anomaly: exit code 0 and no stderr output still means failure if error file exists
            success = False
            self.log.warning("fast_dp exited with error, but with returncode 0")
            self.log.debug(result.stdout)
            self.log.debug(result.stderr)

        if success:
            command = [
                "xia2.report",
                f"log_include={str(working_directory / 'fast_dp.log')}",
                "prefix=fast_dp",
                "title=fast_dp",
                "fast_dp_unmerged.mtz",
            ]
            # run xia2.report in working directory
            self.log.info("Running command: %s", " ".join(command))
            try:
                result = subprocess.run(
                    command,
                    timeout=params.get("timeout"),
                    env=environment,
                    cwd=working_directory,
                )
                runtime = time.perf_counter() - start_time
                self.log.info(f"fast_dp took {runtime} seconds")
                self._runtime_hist.observe(runtime)
            except subprocess.TimeoutExpired as te:
                success = False
                self.log.warning(f"xia2.report timed out: {te.timeout}\n  {te.cmd}")
                self.log.debug(te.stdout)
                self.log.debug(te.stderr)
                self._timeout_counter.inc()
            else:
                success = not result.returncode
                if success:
                    self.log.info("xia2.report successful")
                else:
                    self.log.info(
                        f"xia2.report failed with exitcode {result.returncode}"
                    )
                    self.log.debug(result.stdout)
                    self.log.debug(result.stderr)

            json_file = working_directory / "iotbx-merging-stats.json"
            with json_file.open("w") as fh:
                fh.write(
                    get_merging_statistics(
                        str(working_directory / "fast_dp_unmerged.mtz")
                    ).as_json()
                )

        # Create results directory and symlink if they don't already exist
        results_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                results_directory, params["create_symlink"]
            )
        if pipeine_final_params := params.get("pipeline-final", []):
            final_directory = pathlib.Path(pipeine_final_params["path"])
            final_directory.mkdir(parents=True, exist_ok=True)
            if params.get("create_symlink"):
                dlstbx.util.symlink.create_parent_symlink(
                    final_directory, params["create_symlink"]
                )

            def is_final_result(final_file: pathlib.Path) -> bool:
                return any(
                    fnmatch(str(final_file.name), patt)
                    for patt in pipeine_final_params["patterns"]
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
        output_files = [
            working_directory / "fast_dp-report.html",
            working_directory / "fast_dp.log",
        ]
        output_files.extend(
            set(working_directory.iterdir()).difference(set(output_files))
        )

        allfiles = []
        for filename in output_files:
            if not filename.is_file():
                continue
            filetype = keep_ext.get(filename.suffix)
            if filename.name in keep:
                filetype = keep[filename.name]
            if filetype is None:
                continue
            destination = results_directory / filename.name
            self.log.debug(f"Copying {str(filename)} to {str(destination)}")
            shutil.copy(filename, destination)
            if pipeine_final_params and is_final_result(filename):
                destination = final_directory / filename.name
                shutil.copy(filename, destination)
            allfiles.append(str(destination))

            if filetype:
                self.record_result_individual_file(
                    {
                        "file_path": str(destination.parent),
                        "file_name": destination.name,
                        "file_type": filetype,
                        "importance_rank": (
                            1
                            if destination.name
                            in ("fast_dp.mtz", "fast_dp-report.html")
                            else 2
                        ),
                    }
                )
        if allfiles:
            self.record_result_all_files({"filelist": allfiles})

        # Forward JSON results if possible
        if success and (working_directory / "fast_dp.json").is_file():
            with (working_directory / "fast_dp.json").open("r") as fh:
                json_data = json.load(fh)
            if (
                params.get("store_xtriage_results")
                and (working_directory / "fast_dp-report.json").is_file()
            ):
                with (working_directory / "fast_dp-report.json").open("rb") as fh:
                    xtriage_results = json.load(fh).get("xtriage")
            else:
                xtriage_results = None
            self.send_results_to_ispyb(json_data, xtriage_results=xtriage_results)
        elif success:
            self.log.warning("Expected JSON output file missing")

        if (dc_end_time := params.get("dc_end_time")) and dc_end_time != "None":
            dc_end_time = dateutil.parser.parse(dc_end_time)
            dcid = params.get("dcid")
            latency_s = (datetime.datetime.now() - dc_end_time).total_seconds()
            self.log.info(
                f"fast_dp completed for DCID {dcid} with latency of {latency_s:.2f} seconds",
                extra={"fastdp-latency-seconds": latency_s},
            )
            self._latency_hist.observe(latency_s)

        if success:
            self._success_counter.inc()
        else:
            self._failure_counter.inc()

        return success
