from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

import procrunner
import zocalo.wrapper

from dlstbx.util.iris import get_objects_from_s3

logger = logging.getLogger("zocalo.wrap.xia2_run")


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

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = Path(params.get("working_directory", os.getcwd()))
        working_directory.mkdir(parents=True, exist_ok=True)

        if "s3_urls" in self.recwrap.environment:
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
            try:
                get_objects_from_s3(
                    working_directory, self.recwrap.environment["s3_urls"], logger
                )
            except Exception:
                logger.exception(
                    "Exception raised while downloading files from S3 object store"
                )
                return False

        command = self.construct_commandline(
            working_directory, params, "singularity_image" in self.recwrap.environment
        )
        logger.info("command: %s", " ".join(command))

        procrunner_directory = working_directory / params["create_symlink"]
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
