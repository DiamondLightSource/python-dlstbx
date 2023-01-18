from __future__ import annotations

import datetime
import json
import shutil
from pathlib import Path

import dateutil.parser

import dlstbx.util.symlink
from dlstbx.util.iris import remove_objects_from_s3
from dlstbx.wrapper import Wrapper
from dlstbx.wrapper.helpers import run_dials_estimate_resolution


class Xia2ResultsWrapper(Wrapper):

    _logger_name = "zocalo.wrap.xia2_results"

    def send_results_to_ispyb(
        self, xia2_json, xtriage_results=None, res_i_sig_i_2: float | None = None
    ):
        self.log.info("Reading xia2 results")
        from xia2.Interfaces.ISPyB import xia2_to_json_object
        from xia2.Schema.XProject import XProject

        xinfo = XProject.from_json(filename=str(xia2_json))
        crystals = xinfo.get_crystals()
        assert len(crystals) == 1
        z = xia2_to_json_object(list(crystals.values()))

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

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

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

        working_directory = Path(params["working_directory"])
        results_directory = Path(params["results_directory"])

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

        # copy output files to result directory
        results_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                str(results_directory), params["create_symlink"]
            )

        if not working_directory.is_dir():
            self.log.error(
                f"xia2 working directory {str(working_directory)} not found."
            )
            return False

        for subdir in ("DataFiles", "LogFiles"):
            src = working_directory / subdir
            dst = results_directory / subdir
            if src.exists():
                self.log.debug(f"Recursively copying {str(src)} to {str(dst)}")
                shutil.copytree(src, dst)
            else:
                self.log.warning(
                    f"Expected output directory does not exist: {str(src)}"
                )

        allfiles = []
        for f in working_directory.iterdir():
            if f.is_file() and not f.name.startswith(".") and f.suffix != ".sif":
                self.log.debug(f"Copying {str(f)} to results directory")
                shutil.copy(f, results_directory)
                allfiles.append(str(results_directory / f.name))

        # Send results to various listeners
        logfiles = ("xia2.html", "xia2.txt", "xia2.error", "xia2-error.txt")
        for logfile in logfiles:
            result_file = results_directory / logfile
            if result_file.is_file():
                self.record_result_individual_file(
                    {
                        "file_path": str(result_file.parent),
                        "file_name": result_file.name,
                        "file_type": "log",
                        "importance_rank": 1 if result_file.name == "xia2.html" else 2,
                    }
                )

        success = True
        datafiles_path = results_directory / "DataFiles"
        if datafiles_path.is_dir():
            for result_file in datafiles_path.iterdir():
                if result_file.is_file():
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
                    allfiles.append(str(result_file))
        else:
            self.log.info("xia2 DataFiles directory not found")
            success = False

        logfiles_path = results_directory / "LogFiles"
        if logfiles_path.is_dir():
            for result_file in logfiles_path.iterdir():
                if result_file.is_file():
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
                    allfiles.append(str(result_file))
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
        xia2_report = results_directory / "xia2-report.json"
        xia2_error = results_directory / "xia2.error"
        xia2_error_txt = results_directory / "xia2-error.txt"
        xia2_json = results_directory / "xia2.json"

        if params.get("store_xtriage_results") and xia2_report.is_file():
            with open(xia2_report) as fh:
                xtriage_results = json.load(fh).get("xtriage")
        else:
            xtriage_results = None

        if (
            not (xia2_error_txt.is_file() or xia2_error.is_file())
            and xia2_json.is_file()
            and any(Path(datafiles_path).iterdir())
        ):
            if not params.get("do_not_write_to_ispyb"):
                self.send_results_to_ispyb(
                    xia2_json,
                    xtriage_results=xtriage_results,
                    res_i_sig_i_2=res_i_sig_i_2,
                )
        else:
            self.log.info("xia2 processing exited with and error")
            success = False

        if allfiles:
            self.record_result_all_files({"filelist": allfiles})

        if dc_end_time := params.get("dc_end_time"):
            dc_end_time = dateutil.parser.parse(dc_end_time)
            pipeline = params.get("pipeline", "")
            dcid = params.get("dcid")
            latency_s = (datetime.datetime.now() - dc_end_time).total_seconds()
            program_name = f"xia2-{pipeline}" if pipeline else "xia2"
            self.log.info(
                f"{program_name} completed for DCID {dcid} with latency of {latency_s:.2f} seconds",
                extra={f"{program_name}-latency-seconds": latency_s},
            )

        return success
