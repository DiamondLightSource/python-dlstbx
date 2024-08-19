from __future__ import annotations

import json
import os
import pathlib
import shutil

import procrunner

import dlstbx.util.symlink
from dlstbx.wrapper import Wrapper


class ShelxtWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.shelxt"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        # run in working directory
        working_directory = pathlib.Path(params["working_directory"])
        if not os.path.exists(working_directory):
            os.makedirs(working_directory)
        os.chdir(working_directory)

        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory, params["create_symlink"], levels=1
            )

        # we need the ins and the hkl file here
        ispyb_params = params.get("ispyb_parameters", {})
        previous_directory = ispyb_params.get("ins_file_location", ["."])
        previous_directory = pathlib.Path(previous_directory[0])

        prefix = ispyb_params.get("prefix", ["shelxt_#"])
        if "#" in prefix:
            prefix = prefix[0].split("#")[0][0:-1]
        else:
            prefix = prefix[0].split("_master.h5")[0]

        for f in previous_directory.iterdir():
            if f.is_file() and f.name.startswith("shelx"):
                self.log.info(f"Copying {f} to working directory")
                new_name = prefix + f.suffix
                shutil.copy(f, working_directory / new_name)

        command = [
            "shelxt",
            prefix,
        ]
        self.log.info("shelxt command: %s", " ".join(command))
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
        self.log.debug(result.stdout)

        self.log.info("Shelxt successful, took %.3f seconds", result["runtime"])

        # we want to extract R1 and the space group from the lxt for each solution
        lxt_file_path = working_directory / f"{prefix}.lxt"
        solutions = [["Solution", "Space Group", "R1"]]
        try:
            with open(lxt_file_path, "r") as f:
                lines = f.readlines()
            for i, l in enumerate(lines):
                if l.split() == [
                    "R1",
                    "Rweak",
                    "Alpha",
                    "SysAbs",
                    "Orientation",
                    "Space",
                    "group",
                    "Flack_x",
                    "File",
                    "Formula",
                ]:
                    break
            for j in range(1, 10):
                line = lines[i + j]
                if line == "\n":
                    break
                r1 = line.split()[0]
                spacegroup = line[43:].split()[0]
                filename = line[65:].split()[0]
                solutions.append([filename, spacegroup, r1])
        except Exception as e:
            self.log.warning("could not extract details from lxt file: ", e)

        solutions_filename = working_directory / "shelxt_results.json"
        with open(solutions_filename, "w") as f:
            json.dump(solutions, f)

        # rough hacklet to make a pretty picture
        command2 = [
            "run_csd_python_api",
            "/dls/science/users/fer45166/dials-dev-env/make_a_pretty_picture.py",
            str(working_directory / f"{prefix}_a.res"),
            str(working_directory / f"{prefix}_a.png"),
        ]

        self.log.info("pretty picture command: %s", " ".join(command2))
        result = procrunner.run(command2, timeout=params.get("timeout"))
        if result["exitcode"] or result["timeout"]:
            self.log.warning(
                "Failed to run shelxt with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )

        self.log.info("picture making successful, took %.3f seconds", result["runtime"])

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

        for result_file in results_directory.iterdir():
            if result_file.is_file():
                file_type = "Result"
                if result_file.suffix in [".lxt", ".png", ".json"]:
                    file_type = "Log"
                self.record_result_individual_file(
                    {
                        "file_path": str(result_file.parent),
                        "file_name": result_file.name,
                        "file_type": file_type,
                        "importance_rank": 1,
                    }
                )

        if params.get("create_symlink"):
            # Create symbolic link above working directory
            dlstbx.util.symlink.create_parent_symlink(
                results_directory, params["create_symlink"]
            )

        self.log.info("Done.")

        return True
