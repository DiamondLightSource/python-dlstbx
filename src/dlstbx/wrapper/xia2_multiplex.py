from __future__ import annotations

import itertools
import json
import logging
import os
import shutil
import subprocess
import time
from pathlib import Path

from cctbx import uctbx

import dlstbx.util.iris
import dlstbx.util.symlink
from dlstbx.wrapper import Wrapper


class Xia2MultiplexWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.xia2.multiplex"
    name = "xia2.multiplex"

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
            #'refined_xbeam': z['refined_beam'][0],
            #'refined_ybeam': z['refined_beam'][1],
        }
        ispyb_command_list.append(integration)

        if xtriage_results is not None:
            for level, messages in xtriage_results.items():
                for message in messages:
                    if (
                        message["text"]
                        == "The merging statistics indicate that the data may be assigned to the wrong space group."
                    ):
                        # this is not a useful warning
                        continue
                    ispyb_command_list.append(
                        {
                            "ispyb_command": "add_program_message",
                            "program_id": "$ispyb_autoprocprogram_id",
                            "message": message["text"],
                            "description": message["summary"],
                            "severity": {0: "INFO", 1: "WARNING", 2: "ERROR"}.get(
                                message["level"]
                            ),
                        }
                    )

        self.log.debug(f"Sending {ispyb_command_list}")
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})

    def construct_commandline(self, params):
        """Construct xia2.multiplex command line.
        Takes job parameter dictionary, returns array."""

        command = ["xia2.multiplex"]
        command.extend(params["data"])

        if params.get("ispyb_parameters"):
            ignore = {"sample_id", "sample_group_id"}
            translation = {
                "d_min": "resolution.d_min",
                "spacegroup": "symmetry.space_group",
            }
            for param, value in params["ispyb_parameters"].items():
                if param not in ignore:
                    command.append(translation.get(param, param) + "=" + value[0])

        return command

    def setup(self, working_directory, params):
        # Adjust all paths if a spacegroup is set in ISPyB
        if params.get("ispyb_parameters"):
            if data := params["ispyb_parameters"].pop("data"):
                params["data"] = list(
                    itertools.chain.from_iterable(files.split(";") for files in data)
                )

        assert len(params.get("data", [])) > 1

        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory, params["create_symlink"], levels=1
            )
        if singularity_image := params.get("singularity_image"):
            try:
                dlstbx.util.iris.write_singularity_script(
                    working_directory, singularity_image
                )
                self.recwrap.environment.update(
                    {"singularity_image": singularity_image}
                )
            except Exception:
                self.log.exception("Error writing singularity script")
                return False
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self.log.logger.addHandler(handler)
            self.log.logger.setLevel(logging.DEBUG)
            s3_urls = dlstbx.util.iris.get_presigned_urls_images(
                params.get("create_symlink").lower(),
                params["rpid"],
                params["data"],
                self.log,
            )
            self.recwrap.environment.update({"s3_urls": s3_urls})
        return True

    def run_multiplex(self, working_directory, params):
        # run xia2.multiplex in working directory
        if s3_urls := self.recwrap.environment.get("s3_urls"):
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self.log.logger.addHandler(handler)
            self.log.logger.setLevel(logging.DEBUG)
            try:
                dlstbx.util.iris.get_objects_from_s3(
                    working_directory, s3_urls, self.log
                )
            except Exception:
                self.log.exception(
                    "Exception raised while downloading files from S3 object store"
                )
                return False
            params["data"] = [
                str(working_directory / Path(f).name) for f in params["data"]
            ]
        subprocess_directory = (
            working_directory / params["create_symlink"]
            if params.get("create_symlink")
            else working_directory
        )
        subprocess_directory.mkdir(parents=True, exist_ok=True)

        success = True
        command = self.construct_commandline(params)
        self.log.info(f"command: {' '.join(command)}")
        try:
            start_time = time.perf_counter()
            result = subprocess.run(
                command,
                capture_output=True,
                timeout=params.get("timeout"),
                cwd=subprocess_directory,
            )
            runtime = time.perf_counter() - start_time
            self.log.info(f"xia2.multiplex took {runtime} seconds")
            self._runtime_hist.observe(runtime)
        except subprocess.TimeoutExpired as te:
            success = False
            self.log.warning(f"xia2 timed out: {te.timeout}\n  {te.cmd}")
            self.log.debug(te.stdout)
            self.log.debug(te.stderr)
            self._timeout_counter.inc()
        else:
            if success := not result.returncode:
                self.log.info("xia2.multiplex successful")
            else:
                self.log.info(
                    f"xia2.multiplex failed with exitcode {result.returncode}"
                )
                self.log.debug(result.stdout)
                self.log.debug(result.stderr)
        self.log.info(f"working_directory: {working_directory}")

        return success

    def report(self, working_directory, params, success):
        if s3_urls := self.recwrap.environment.get("s3_urls"):
            try:
                dlstbx.util.iris.remove_objects_from_s3(
                    params.get("create_symlink").lower(),
                    s3_urls,
                )
            except Exception:
                self.log.exception(
                    "Exception raised while trying to remove files from S3 object store."
                )
        if params.get("create_symlink"):
            working_directory = working_directory / params["create_symlink"]
        scaled_unmerged_mtz = working_directory / "scaled_unmerged.mtz"
        if success and scaled_unmerged_mtz.is_file():
            import iotbx.merging_statistics
            i_obs = iotbx.merging_statistics.select_data(
                os.fspath(scaled_unmerged_mtz), data_labels=None
            )
        else:
            success = False
        multiplex_json = working_directory / "xia2.multiplex.json"
        if success and multiplex_json.is_file():
            with multiplex_json.open("r") as fh:
                d = json.load(fh)
            merging_stats = d["datasets"]["All data"]["merging_stats"]
            merging_stats_anom = d["datasets"]["All data"]["merging_stats_anom"]
            with (working_directory / "merging-stats.json").open("w") as fh:
                json.dump(merging_stats, fh)
            def lookup(merging_stats, item, shell):
                i_bin = {"innerShell": 0, "outerShell": -1}.get(shell)
                if i_bin is not None:
                    return merging_stats[item][i_bin]
                return merging_stats["overall"][item]
            if not command:
                command = self.construct_commandline(params)
            ispyb_d = {
                "commandline": " ".join(command),
                "spacegroup": i_obs.space_group().type().lookup_symbol(),
                "unit_cell": list(i_obs.unit_cell().parameters()),
                "scaling_statistics": {},
            }
            for shell in ("overall", "innerShell", "outerShell"):
                ispyb_d["scaling_statistics"][shell] = {
                    "cc_half": lookup(merging_stats, "cc_one_half", shell),
                    "completeness": lookup(merging_stats, "completeness", shell),
                    "mean_i_sig_i": lookup(
                        merging_stats, "i_over_sigma_mean", shell
                    ),
                    "multiplicity": lookup(merging_stats, "multiplicity", shell),
                    "n_tot_obs": lookup(merging_stats, "n_obs", shell),
                    "n_tot_unique_obs": lookup(merging_stats, "n_uniq", shell),
                    "r_merge": lookup(merging_stats, "r_merge", shell),
                    "res_lim_high": uctbx.d_star_sq_as_d(
                        lookup(merging_stats, "d_star_sq_min", shell)
                    ),
                    "res_lim_low": uctbx.d_star_sq_as_d(
                        lookup(merging_stats, "d_star_sq_max", shell)
                    ),
                    "anom_completeness": lookup(
                        merging_stats_anom, "anom_completeness", shell
                    ),
                    "anom_multiplicity": lookup(
                        merging_stats_anom, "multiplicity", shell
                    ),
                    "cc_anom": lookup(merging_stats_anom, "cc_anom", shell),
                    "r_meas_all_iplusi_minus": lookup(
                        merging_stats_anom, "r_meas", shell
                    ),
                }
            xtriage_results = d["datasets"]["All data"].get("xtriage")
        else:
            success = False

        # copy output files to result directory
        results_directory = Path(params["results_directory"])
        results_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                results_directory, params["create_symlink"], levels=1
            )
            results_directory = (
                Path(params["results_directory"]) / params["create_symlink"]
            )
            results_directory.mkdir(parents=True, exist_ok=True)
        keep_ext = {
            ".png": None,
            ".log": "log",
            ".json": None,
            ".pickle": None,
            ".expt": None,
            ".refl": None,
            ".mtz": None,
            ".html": "log",
        }
        keep = {
            "scaled.mtz": "result",
            "scaled_unmerged.mtz": "result",
            "scaled.expt": "result",
            "scaled.refl": "result",
            "merging-stats.json": "graph",
            "xia2.multiplex.json": "result",
        }
        # Record these log files first so they appear at the top of the list
        # of attachments in SynchWeb
        primary_log_files = [
            working_directory / "xia2.multiplex.html",
            working_directory / "xia2.multiplex.log",
        ]
        allfiles = []
        for filename in primary_log_files + list(working_directory.iterdir()):
            if not filename.is_file():
                continue  # primary_log_files may not actually exist
            filetype = keep_ext.get(filename.suffix)
            if filename.name in keep:
                filetype = keep[filename.name]
            if filetype is None:
                continue
            destination = results_directory / filename.name
            if os.fspath(destination) in allfiles:
                # We've already seen this file above
                continue
            self.log.debug(f"Copying {filename} to {destination}")
            allfiles.append(os.fspath(destination))
            shutil.copy(filename, destination)
            if filetype:
                self.record_result_individual_file(
                    {
                        "file_path": os.fspath(destination.parent),
                        "file_name": destination.name,
                        "file_type": filetype,
                        "importance_rank": 1
                        if destination.name in ("scaled.mtz", "xia2.multiplex.html")
                        else 2,
                    }
                )
        if allfiles:
            self.record_result_all_files({"filelist": allfiles})
        if success:
            self.send_results_to_ispyb(ispyb_d, xtriage_results=xtriage_results)

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
                and "/" not in params["ispyb_parameters"]["spacegroup"][0]
                and "create_symlink" in params
            ):
                params["create_symlink"] += (
                    "-" + params["ispyb_parameters"]["spacegroup"][0]
                )

        stage = params.get("stage")
        assert stage in {None, "setup", "run", "report"}
        success = True

        if stage in {None, "setup"}:
            success = self.setup(working_directory, params)

        if stage in {None, "run"} and success:
            success = self.run_multiplex(working_directory, params)

        if stage in {None, "report"} and success:
            success = self.report(working_directory, params, success)
            if success:
                self._success_counter.inc()

        if not success:
            self._failure_counter.inc()

        return success
