import logging
import os
from argparse import Namespace
from pathlib import Path
from pprint import pformat

import procrunner
import zocalo
from jinja2.environment import Environment
from jinja2.exceptions import UndefinedError
from jinja2.loaders import PackageLoader

logger = logging.getLogger("dlstbx.wrap.big_ep_run")


class BigEPRunWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        self.recwrap.environment.update(params["ispyb_parameters"])

        # Collect parameters from payload and check them
        self.msg = Namespace(**self.recwrap.environment["msg"])

        working_directory = Path(params.get("working_directory", os.getcwd()))
        working_directory.mkdir(parents=True, exist_ok=True)
        self.msg.wd = str(working_directory)

        pipeline = self.recwrap.environment.get("pipeline")

        tmpl_env = Environment(loader=PackageLoader("dlstbx.util", "big_ep_templates"))
        pipeline_template = tmpl_env.get_template(f"{pipeline}.sh")
        pipeline_script = working_directory / f"run_{pipeline}.sh"

        self.msg.singularity_image = params.get("singularity_image")

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
            working_directory=working_directory,
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
