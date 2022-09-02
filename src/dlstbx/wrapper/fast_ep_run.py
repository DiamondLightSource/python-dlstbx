from __future__ import annotations

import os
from pathlib import Path

import procrunner

from dlstbx.wrapper import Wrapper


class FastEPRunWrapper(Wrapper):

    _logger_name = "zocalo.wrap.fast_ep_run"

    def construct_commandline(self, params):
        """Construct fast_ep command line.
        Takes job parameter dictionary, returns array."""

        command = ["fast_ep"]
        for param, value in params["fast_ep"].items():
            if value:
                self.log.info(f"Parameter {param}: {value}")
                if param == "rlims":
                    value = ",".join(str(r) for r in value)
                command.append(f"{param}={value}")

        return command

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]

        # Create working directory with symbolic link
        working_directory = Path(params.get("working_directory", os.getcwd()))
        working_directory.mkdir(parents=True, exist_ok=True)

        if params.get("ispyb_parameters"):
            if params["ispyb_parameters"].get("data"):
                if "singularity_image" in self.recwrap.environment:
                    params["fast_ep"]["data"] = working_directory / os.path.basename(
                        params["ispyb_parameters"]["data"]
                    )
                else:
                    params["fast_ep"]["data"] = os.path.abspath(
                        params["ispyb_parameters"]["data"]
                    )

        command = self.construct_commandline(params)
        procrunner_directory = working_directory / params["create_symlink"]
        procrunner_directory.mkdir(parents=True, exist_ok=True)

        result = procrunner.run(
            command,
            timeout=params.get("timeout"),
            working_directory=procrunner_directory,
        )
        self.log.info("command: %s", " ".join(result["command"]))
        self.log.info("runtime: %s", result["runtime"])
        success = (
            not result["exitcode"]
            and not result["timeout"]
            and not Path(procrunner_directory / "fast_ep.error").exists()
        )
        if success:
            self.log.info("fast_ep successful, took %.1f seconds", result["runtime"])
        else:
            self.log.info(
                "fast_ep failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            self.log.debug(result["stdout"])
            self.log.debug(result["stderr"])

        return success
