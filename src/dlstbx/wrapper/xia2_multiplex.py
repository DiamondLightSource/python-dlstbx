from __future__ import annotations

import itertools
import json
import pathlib
import shutil
import subprocess
import time
from fnmatch import fnmatch

import iotbx.merging_statistics
from cctbx import uctbx

import dlstbx.util.symlink
from dlstbx.wrapper import Wrapper


def lookup(merging_stats, item, shell):
    i_bin = {"innerShell": 0, "outerShell": -1}.get(shell)
    if i_bin is not None:
        return merging_stats[item][i_bin]
    return merging_stats["overall"][item]


class Xia2MultiplexWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.xia2.multiplex"
    name = "xia2.multiplex"

    def send_results_to_ispyb(
        self, z, xtriage_results=None, cluster_num=None, attachments=[]
    ):
        ispyb_command_list = []

        # Step 0: For clusters, register new AutoProcProgram record
        if cluster_num is not None:
            register_autoproc_prog = {
                "ispyb_command": "register_processing",
                "program": "xia2.multiplex",
                "cmdline": "xia2.multiplex (ap-zoc)",
                "environment": {"cluster": cluster_num},
                "store_result": "ispyb_autoprocprogram_id",
            }
            ispyb_command_list.append(register_autoproc_prog)

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
        # Use pre-registered integration id for 'All data' dataset
        if cluster_num is not None:
            integration_id = None
        else:
            integration_id = "$ispyb_integration_id"

        integration = {
            "ispyb_command": "upsert_integration",
            "scaling_id": "$ispyb_autoprocscaling_id",
            "integration_id": integration_id,
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

        # Step 4: Upload attachments
        if attachments:
            for attachment in attachments:
                upload_attachment = {
                    "ispyb_command": "add_program_attachment",
                    "program_id": "$ispyb_autoprocprogram_id",
                    "file_name": attachment["file_name"],
                    "file_path": attachment["file_path"],
                    "file_type": attachment["file_type"],
                    "importance_rank": attachment["importance_rank"],
                }
                ispyb_command_list.append(upload_attachment)

        # Step 5: Register successful processing for cluster jobs
        if cluster_num is not None:
            update_autoproc_prog = {
                "ispyb_command": "update_processing_status",
                "program_id": "$ispyb_autoprocprogram_id",
                "message": "processing successful",
                "status": "success",
            }
            ispyb_command_list.append(update_autoproc_prog)

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

        self.log.debug("Sending %s", str(ispyb_command_list))
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})

    def construct_commandline(self, params):
        """Construct xia2.multiplex command line.
        Takes job parameter dictionary, returns array."""

        command = ["xia2.multiplex"]
        data_files = itertools.chain.from_iterable(
            files.split(";") for files in params["data"]
        )
        for f in data_files:
            command.append(f)

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

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

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
            if data := params["ispyb_parameters"].pop("data"):
                params["data"] = data

        assert len(params.get("data", [])) > 1

        command = self.construct_commandline(params)

        working_directory = pathlib.Path(params["working_directory"])
        results_directory = pathlib.Path(params["results_directory"])

        # Create working directory with symbolic link
        working_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory, params["create_symlink"]
            )

        # run xia2.multiplex in working directory
        self.log.info("command: %s", " ".join(command))
        try:
            start_time = time.perf_counter()
            result = subprocess.run(
                command,
                capture_output=True,
                timeout=params.get("timeout"),
                cwd=working_directory,
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

        scaled_unmerged_mtz = working_directory / "scaled_unmerged.mtz"
        multiplex_json = working_directory / "xia2.multiplex.json"

        # Placeholder logic to keep existing functionality - TODO - review if this is needed still
        if not (scaled_unmerged_mtz.is_file() and multiplex_json.is_file()):
            success = False

        # Create results directory
        results_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                results_directory, params["create_symlink"]
            )
        if pipeline_final_params := params.get("pipeline-final", []):
            final_directory = pathlib.Path(pipeline_final_params["path"])
            final_directory.mkdir(parents=True, exist_ok=True)
            if params.get("create_symlink"):
                dlstbx.util.symlink.create_parent_symlink(
                    final_directory, params["create_symlink"]
                )

            def is_final_result(final_file: pathlib.Path) -> bool:
                return any(
                    fnmatch(str(final_file.name), patt)
                    for patt in pipeline_final_params["patterns"]
                )

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
        output_files = [
            working_directory / "xia2.multiplex.html",
            working_directory / "xia2.multiplex.log",
        ]

        allfiles = []

        if success:
            with multiplex_json.open("r") as fh:
                d = json.load(fh)

            for dataset_name, dataset in d["datasets"].items():
                if dataset_name == "All data":
                    base_dir = working_directory
                    scaled_unmerged_mtz = base_dir / "scaled_unmerged.mtz"
                    scaled_mtz = working_directory / "scaled.mtz"
                    dimple_symlink = "dimple-xia2.multiplex"
                    cluster_prefix = ""
                    cluster_num = None
                elif "coordinate cluster" in dataset_name:
                    cluster_num = dataset_name.split(" ")[-1]
                    cluster_prefix = f"coordinate_cluster_{cluster_num}_"
                    base_dir = working_directory / f"coordinate_cluster_{cluster_num}"
                    scaled_unmerged_mtz = (
                        base_dir / f"{cluster_prefix}scaled_unmerged.mtz"
                    )
                    scaled_mtz = results_directory / f"{cluster_prefix}scaled.mtz"
                    dimple_symlink = (
                        f"dimple-xia2.multiplex-coordinate_cluster_{cluster_num}"
                    )
                else:
                    self.log.warning(
                        f"Ignoring unrecognised dataset pattern {dataset_name}"
                    )
                    continue

                i_obs = iotbx.merging_statistics.select_data(
                    scaled_unmerged_mtz.as_posix(), data_labels=None
                )

                merging_stats = dataset["merging_stats"]
                merging_stats_anom = dataset["merging_stats_anom"]
                with (base_dir / f"{cluster_prefix}merging-stats.json").open("w") as fh:
                    json.dump(merging_stats, fh)

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

                xtriage_results = dataset.get("xtriage")
                attachments = []

                for filename in list(base_dir.iterdir()):
                    if filename not in output_files:
                        output_files.append(filename)

                for filename in output_files:
                    filetype = None
                    if not filename.is_file():
                        continue  # primary log files may not actually exist
                    filetype = keep_ext.get(filename.suffix)
                    for file_pattern in keep:
                        if filename.name.endswith(file_pattern):
                            filetype = keep[file_pattern]
                    if filetype is None:
                        continue
                    destination = results_directory / filename.name
                    if filename.as_posix() not in allfiles:
                        allfiles.append(filename.as_posix())
                        self.log.debug(f"Copying {filename} to {destination}")
                        shutil.copy(filename, destination)
                    if pipeline_final_params and is_final_result(filename):
                        destination = final_directory / filename.name
                        if filename.as_posix() not in allfiles:
                            self.log.debug(f"Copying {filename} to {destination}")
                            shutil.copy(filename, destination)

                    # Files uploaded separately for each cluster
                    if filetype:
                        attachments.append(
                            {
                                "file_path": destination.parent.as_posix(),
                                "file_name": destination.name,
                                "file_type": filetype,
                                "importance_rank": (
                                    1
                                    if destination.name.endswith(
                                        ("scaled.mtz", "xia2.multiplex.html")
                                    )
                                    else 2
                                ),
                            }
                        )
                # Add parameters to the environment to be picked up downstream by trigger function
                self.recwrap.environment.update({"scaled_mtz": scaled_mtz.as_posix()})
                self.recwrap.environment.update(
                    {"scaled_unmerged_mtz": scaled_unmerged_mtz.as_posix()}
                )
                self.recwrap.environment.update({"dimple_symlink": dimple_symlink})

                # Send results to ispyb and trigger downstream recipe steps for this dataset
                self.log.info(
                    f"Triggering downstream recipe steps for dataset: '{dataset_name}'"
                )
                self.send_results_to_ispyb(
                    ispyb_d,
                    xtriage_results=xtriage_results,
                    cluster_num=cluster_num,
                    attachments=attachments,
                )

            self._success_counter.inc()
        else:
            self._failure_counter.inc()

        return success
