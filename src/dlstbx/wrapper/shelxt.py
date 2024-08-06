from __future__ import annotations

import os
import shutil
import pathlib

import procrunner

import dlstbx.util.symlink
from dlstbx.wrapper import Wrapper


class ShelxtWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.shelxt"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        self.log.info(f"params from recipe: {params}")
        # run in working directory
        working_directory = pathlib.Path(params["working_directory"])
        if not os.path.exists(working_directory):
            os.makedirs(working_directory)
        os.chdir(working_directory)

        # we need the ins and the hkl file here
        ispyb_params = params.get("ispyb_parameters", {})
        previous_directory = ispyb_params.get("ins_file_location", ["."])
        previous_directory = pathlib.Path(previous_directory[0])
        self.log.info(previous_directory)
        for f in previous_directory.iterdir():
            if f.is_file() and f.name.startswith("shelx"):
                self.log.info(f"Copying {f} to working directory")
                shutil.copy(f, working_directory)

        command = [
            "shelxt",
            "shelxt",  # it appears as though the file is always called shelxt.ins?
        ]
        self.log.info("command: %s", " ".join(command))
        result = procrunner.run(command, timeout=params.get("timeout"))
        if result["exitcode"] or result["timeout"]:
            self.log.warning(
                "Failed to run shelxt with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            return False
        # shelxt returns 0 if it didn't find the file :eyeroll:
        # lets check the stdout to see if anything's up
        self.log.info(result.stdout)

        self.log.info("Shelxt successful, took %.3f seconds", result["runtime"])

        # copy output files to result directory
        results_directory = pathlib.Path(params["results_directory"])
        self.log.info(previous_directory)
        if not os.path.exists(results_directory):
            os.makedirs(results_directory)

        # copy some/all outputs to the results directory
        for f in working_directory.iterdir():
            if f.is_file() and not f.name.startswith("."):
                self.log.debug(f"Copying {f} to results directory")
                shutil.copy(f, results_directory)

        # Send results to various listeners
        shelxt_files = ["shelxt_a.hkl", "shelxt_a.res", "shelxt.hkl", "shelxt.ins", "shelxt.lxt"]
        for result_file in [results_directory / x for x in shelxt_files]:
            if result_file.is_file():
                file_type = "Result"
                if result_file.name.endswith("lxt"):
                    file_type = "Log"
                elif result_file.stem.endswith("_a"):
                    file_type = "Input"
                self.record_result_individual_file(
                    {
                        "file_path": str(result_file.parent),
                        "file_name": result_file.name,
                        "file_type": file_type,
                        "importance_rank": 1,
                    }
                )

        if params.get("results_symlink"):
            # Create symbolic link above working directory
            dlstbx.util.symlink.create_parent_symlink(
                results_directory, params["results_symlink"]
            )

        self.log.info("Done.")

        return True
