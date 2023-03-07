from __future__ import annotations

import datetime
import json
import logging
import os
import shutil
import subprocess
import time
from pathlib import Path

import dateutil.parser

import dlstbx.util.symlink
from dlstbx.util.iris import (
    get_image_files,
    get_objects_from_s3,
    get_presigned_urls_images,
    remove_objects_from_s3,
    write_singularity_script,
)
from dlstbx.wrapper import Wrapper
from dlstbx.wrapper.helpers import run_dials_estimate_resolution


class Xia2Wrapper(Wrapper):
    _logger_name = "dlstbx.wrap.xia2"
    name = "xia2"

    def construct_commandline(self, working_directory, params, is_cloud=False):
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
            if param == "image" and is_cloud:
                update_values = []
                for val in values:
                    pth, sweep = val.split(":", 1)
                    cloud_path = str(working_directory / Path(pth).name)
                    update_values.append(":".join([cloud_path, sweep]))
                values = update_values
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

    def send_results_to_ispyb(
        self, xtriage_results=None, res_i_sig_i_2: float | None = None
    ):
        self.log.info("Reading xia2 results")
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
        if res_i_sig_i_2 is not None:
            insert_scaling["overall"]["res_i_sig_i_2"] = res_i_sig_i_2
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

        self.log.info("Sending %s", str(ispyb_command_list))
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        self.log.info("Sent %d commands to ISPyB", len(ispyb_command_list))

    def setup(self, working_directory, params):

        # Create symbolic link
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory, params["create_symlink"], levels=1
            )

        singularity_image = params.get("singularity_image")
        if singularity_image:
            try:
                tmp_path = working_directory / "TMP"
                tmp_path.mkdir(parents=True, exist_ok=True)
                # shutil.copy(singularity_image, str(working_directory))
                # image_name = Path(singularity_image).name
                write_singularity_script(
                    working_directory, singularity_image, tmp_path.name
                )
                self.recwrap.environment.update(
                    {"singularity_image": singularity_image}
                )
            except Exception:
                self.log.exception("Error writing singularity script")
                return False

            if params.get("s3_urls"):
                formatter = logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
                handler = logging.StreamHandler()
                handler.setFormatter(formatter)
                self.log.logger.addHandler(handler)
                self.log.logger.setLevel(logging.DEBUG)
                s3_urls = get_presigned_urls_images(
                    params.get("create_symlink").lower(),
                    params["rpid"],
                    params["images"],
                    self.log,
                )
                self.recwrap.environment.update({"s3_urls": s3_urls})
            else:
                image_files = get_image_files(
                    working_directory, params["images"], self.log
                )
                self.recwrap.environment.update(
                    {"htcondor_upload_images": ",".join(image_files.keys())}
                )

        return True

    def run_xia2(self, working_directory, params):
        if "s3_urls" in self.recwrap.environment:
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self.log.logger.addHandler(handler)
            self.log.logger.setLevel(logging.DEBUG)
            try:
                get_objects_from_s3(
                    working_directory, self.recwrap.environment["s3_urls"], self.log
                )
            except Exception:
                self.log.exception(
                    "Exception raised while downloading files from S3 object store"
                )
                return False

        command = self.construct_commandline(
            working_directory, params, "singularity_image" in self.recwrap.environment
        )
        self.log.info("command: %s", " ".join(command))

        procrunner_directory = working_directory / params["program_name"]
        procrunner_directory.mkdir(parents=True, exist_ok=True)

        if "dials.integrate.phil_file" in params["xia2"]:
            dials_integrate_phil_file = procrunner_directory / params["xia2"].get(
                "dials.integrate.phil_file"
            )
            max_memory_usage = params["dials.integrate.phil_file"].get(
                "max_memory_usage", 0.9
            )
            with open(dials_integrate_phil_file, "w") as fp:
                fp.write(
                    f"""integration {{
       block {{
         max_memory_usage = {max_memory_usage}
       }}
    }}"""
                )

        try:
            start_time = time.perf_counter()
            result = subprocess.run(
                command,
                timeout=params.get("timeout"),
                cwd=procrunner_directory,
            )
            runtime = time.perf_counter() - start_time
            self.log.info(f"xia2 took {runtime} seconds")
            self._runtime_hist.observe(runtime)
        except subprocess.TimeoutExpired as te:
            success = False
            self.log.warning(f"xia2 timed out: {te.timeout}\n  {te.cmd}")
            self.log.debug(te.stdout)
            self.log.debug(te.stderr)
            self._timeout_counter.inc()
        else:
            success = not result.returncode
            if success:
                self.log.info("xia2 successful")
            else:
                self.log.info(f"xia2 failed with exitcode {result.returncode}")
                self.log.debug(result.stdout)
                self.log.debug(result.stderr)

        return success

    def report(self, working_directory, params, success):
        # copy output files to result directory
        if "s3_urls" in self.recwrap.environment:
            try:
                remove_objects_from_s3(
                    params.get("create_symlink").lower(),
                    self.recwrap.environment.get("s3_urls"),
                )
            except Exception:
                self.log.exception(
                    "Exception raised while trying to remove files from S3 object store."
                )

        working_directory = working_directory / params["program_name"]
        if not working_directory.is_dir():
            self.log.error(
                f"xia2 working directory {str(working_directory)} not found."
            )
            return False

        results_directory = Path(params["results_directory"]) / params["program_name"]
        results_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                results_directory, params["create_symlink"]
            )

        for subdir in ("DataFiles", "LogFiles"):
            src = working_directory / subdir
            dst = results_directory / subdir
            if src.exists():
                self.log.debug(f"Recursively copying {src} to {dst}")
                shutil.copytree(src, dst)
            elif not success:
                self.log.info(
                    f"Expected output directory does not exist (non-zero exitcode): {src}"
                )
            else:
                self.log.warning(f"Expected output directory does not exist: {src}")

        allfiles = []
        for f in working_directory.iterdir():
            if f.is_file() and not f.name.startswith(".") and f.suffix != ".sif":
                self.log.debug(f"Copying {str(f)} to results directory")
                shutil.copy(f, results_directory)
                allfiles.append(str(results_directory / f.name))

        # Send results to various listeners
        logfiles = ("xia2.html", "xia2.txt", "xia2.error", "xia2-error.txt")
        for result_file in map(results_directory.joinpath, logfiles):
            if result_file.is_file():
                self.record_result_individual_file(
                    {
                        "file_path": str(result_file.parent),
                        "file_name": result_file.name,
                        "file_type": "log",
                        "importance_rank": 1 if result_file.name == "xia2.html" else 2,
                    }
                )

        datafiles_path = results_directory / "DataFiles"
        if datafiles_path.is_dir():
            for result_file in datafiles_path.iterdir():
                if not result_file.is_file():
                    continue
                file_type = "result"
                if result_file.suffix in (".log", ".txt"):
                    file_type = "log"
                self.record_result_individual_file(
                    {
                        "file_path": str(result_file.parent),
                        "file_name": result_file.name,
                        "file_type": file_type,
                        "importance_rank": 1
                        if result_file.name.endswith("_free.mtz")
                        else 2,
                    }
                )
                allfiles.append(os.fspath(result_file))
        else:
            self.log.info("xia2 DataFiles directory not found")
            success = False

        logfiles_path = results_directory / "LogFiles"
        if logfiles_path.exists():
            for result_file in logfiles_path.iterdir():
                if not result_file.is_file():
                    continue
                file_type = "log"
                if result_file.suffix == ".json":
                    file_type = "graph"
                elif result_file.suffix == ".png":
                    file_type = "log"
                self.record_result_individual_file(
                    {
                        "file_path": str(result_file.parent),
                        "file_name": result_file.name,
                        "file_type": file_type,
                        "importance_rank": 2,
                    }
                )
                allfiles.append(os.fspath(result_file))
        else:
            self.log.info("xia2 LogFiles directory not found")
            success = False

        # Calculate the resolution at which the mean merged I/sig(I) = 2
        # Why? Because https://jira.diamond.ac.uk/browse/LIMS-104
        res_i_sig_i_2 = None
        if success:
            try:
                estimate_resolution_input_files = [
                    next((working_directory / "DataFiles").glob("*_scaled.expt")),
                    next((working_directory / "DataFiles").glob("*_scaled.refl")),
                ]
            except StopIteration:
                estimate_resolution_input_files = [
                    next(
                        (working_directory / "DataFiles").glob("*_scaled_unmerged.mtz")
                    ),
                ]
            try:
                extra_args = ["misigma=2"]
                resolution_limits = run_dials_estimate_resolution(
                    estimate_resolution_input_files,
                    working_directory,
                    extra_args=extra_args,
                )
                res_i_sig_i_2 = resolution_limits.get("Mn(I/sig)")
            except Exception as e:
                self.log.warning(
                    f"dials.estimate_resolution failure: {e}", exc_info=True
                )

        # Part of the result parsing requires to be in result directory
        os.chdir(results_directory)
        if params.get("store_xtriage_results") and os.path.isfile("xia2-report.json"):
            with open("xia2-report.json") as fh:
                xtriage_results = json.load(fh).get("xtriage")
        else:
            xtriage_results = None
        if (
            success
            and not (os.path.isfile("xia2-error.txt") or os.path.isfile("xia2.error"))
            and os.path.exists("xia2.json")
            and not params.get("do_not_write_to_ispyb")
        ):
            self.send_results_to_ispyb(
                xtriage_results=xtriage_results, res_i_sig_i_2=res_i_sig_i_2
            )
        os.chdir(working_directory)

        if allfiles:
            self.record_result_all_files({"filelist": allfiles})

        if dc_end_time := params.get("dc_end_time"):
            dc_end_time = dateutil.parser.parse(dc_end_time)
            dcid = params.get("dcid")
            latency_s = (datetime.datetime.now() - dc_end_time).total_seconds()
            self.log.info(
                f"{params['program_name']} completed for DCID {dcid} with latency of {latency_s:.2f} seconds",
                extra={f"{params['program_name']}-latency-seconds": latency_s},
            )

        return success

    def run(self):

        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]

        # Create working directory with symbolic link
        working_directory = Path(params.get("working_directory", os.getcwd()))
        working_directory.mkdir(parents=True, exist_ok=True)

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

        stage = params.get("stage")
        assert stage in {None, "setup", "run", "report"}
        if stage in {None, "run", "report"}:
            pipeline = params["xia2"].get("pipeline")
            params["program_name"] = f"xia2-{pipeline}" if pipeline else "xia2"

        success = True

        if stage in {None, "setup"}:
            success = self.setup(working_directory, params)

        if stage in {None, "run"} and success:
            success = self.run_xia2(working_directory, params)

        if stage in {None, "report"} and success:
            success = self.report(working_directory, params, success)

        return success
