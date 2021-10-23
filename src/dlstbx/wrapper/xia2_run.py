import logging
import os
import subprocess
from pathlib import Path

import procrunner
import requests
import zocalo.wrapper

logger = logging.getLogger("dlstbx.wrap.xia2_run")


class Xia2RunWrapper(zocalo.wrapper.BaseWrapper):
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
                values = [str(working_directory / val) for val in values]
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

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = Path(params.get("working_directory", os.getcwd()))
        working_directory.mkdir(parents=True, exist_ok=True)

        is_cloud = False
        try:
            s3_urls = self.recwrap.payload["s3_urls"]
            for filename, s3_url in s3_urls.items():
                file_data = requests.get(s3_url)
                filepath = working_directory / filename
                with open(filepath, "wb") as fp:
                    fp.write(file_data.content)
                is_cloud = True
        except (KeyError, TypeError):
            logger.error("Cannot read input files from S3 store.")

        command = self.construct_commandline(working_directory, params, is_cloud)
        logger.info("command: %s", " ".join(command))

        procrunner_directory = working_directory / "-".join(
            ["xia2", params["xia2"]["pipeline"]]
        )
        procrunner_directory.mkdir(parents=True, exist_ok=True)
        try:
            result = procrunner.run(
                command,
                timeout=params.get("timeout"),
                raise_timeout_exception=True,
                working_directory=str(procrunner_directory),
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

        return success
