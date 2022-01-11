from __future__ import annotations

import logging
import os
import shutil
from argparse import Namespace
from pathlib import Path
from pprint import pformat

import procrunner
import zocalo
from jinja2.environment import Environment
from jinja2.exceptions import UndefinedError
from jinja2.loaders import PackageLoader

from dlstbx.util.big_ep_helpers import write_sequence_file, write_settings_file

logger = logging.getLogger("zocalo.wrap.big_ep_run")


class BigEPRunWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        self.recwrap.environment.update(params["ispyb_parameters"])

        # Collect parameters from payload and check them
        self.msg = Namespace(**self.recwrap.environment["msg"])

        pipeline = self.recwrap.environment.get("pipeline")

        working_directory = Path(params.get("working_directory", os.getcwd()))
        working_directory.mkdir(parents=True, exist_ok=True)
        output_directory = working_directory / pipeline
        output_directory.mkdir(parents=True, exist_ok=True)

        input_mtz = Path(params["ispyb_parameters"]["data"]).name
        shutil.move(working_directory / input_mtz, output_directory)
        self.msg.wd = str(output_directory)

        tmpl_env = Environment(loader=PackageLoader("dlstbx.util", "big_ep_templates"))
        pipeline_template = tmpl_env.get_template(f"{pipeline}.sh")
        pipeline_script = output_directory / f"run_{pipeline}.sh"

        self.msg.singularity_image = params.get("singularity_image")

        try:
            write_sequence_file(output_directory, self.msg)
        except Exception:
            logger.exception("Error writing sequence file")
        try:
            write_settings_file(output_directory, self.msg)
        except Exception:
            logger.exception("Error reading big_ep parameters")

        logger.info(f"Message object: {pformat(self.msg)}")
        logger.info(f"Parameters: {params}")
        with open(pipeline_script, "w") as fp:
            try:
                pipeline_input = pipeline_template.render(self.msg.__dict__)
            except UndefinedError:
                logger.exception(f"Error rendering {pipeline} script template")
                return False
            fp.write(pipeline_input)

        result = procrunner.run(
            ["sh", pipeline_script],
            timeout=params.get("timeout"),
            working_directory=output_directory,
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("runtime: %s", result["runtime"])

        # Just log exit state of the program and try to read any
        # intermediate models in case of failure/timeout
        success = not result["exitcode"] and not result["timeout"]
        if success:
            logger.info(f"{pipeline} successful, took %.1f seconds", result["runtime"])
        else:
            logger.info(
                f"{pipeline} failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            logger.debug(result["stdout"])
            logger.debug(result["stderr"])

        return True
