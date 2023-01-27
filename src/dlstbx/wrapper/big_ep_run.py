from __future__ import annotations

import os
import shutil
from argparse import Namespace
from pathlib import Path
from pprint import pformat

import procrunner
from jinja2.environment import Environment
from jinja2.exceptions import UndefinedError
from jinja2.loaders import PackageLoader

from dlstbx.util.big_ep_helpers import write_sequence_file, write_settings_file
from dlstbx.wrapper import Wrapper


class BigEPRunWrapper(Wrapper):

    _logger_name = "zocalo.wrap.big_ep_run"

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
            self.log.exception("Error writing sequence file")
        try:
            write_settings_file(output_directory, self.msg)
        except Exception:
            self.log.exception("Error reading big_ep parameters")

        self.log.info(f"Message object: {pformat(self.msg)}")
        self.log.info(f"Parameters: {params}")
        with open(pipeline_script, "w") as fp:
            try:
                pipeline_input = pipeline_template.render(self.msg.__dict__)
            except UndefinedError:
                self.log.exception(f"Error rendering {pipeline} script template")
                return False
            fp.write(pipeline_input)

        result = procrunner.run(
            ["sh", pipeline_script],
            timeout=params.get("timeout"),
            working_directory=output_directory,
        )
        self.log.info("command: %s", " ".join(result["command"]))
        self.log.info("runtime: %s", result["runtime"])

        # Just log exit state of the program and try to read any
        # intermediate models in case of failure/timeout
        success = not result["exitcode"] and not result["timeout"]
        if success:
            self.log.info(
                f"{pipeline} successful, took %.1f seconds", result["runtime"]
            )
        else:
            self.log.info(
                f"{pipeline} failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            self.log.debug(result["stdout"])
            self.log.debug(result["stderr"])

        # HTCondor resolves symlinks while transferring data and doesn't support symlinks to direcotries
        if self.msg.singularity_image:
            for tmp_file in output_directory.rglob("*"):
                if (
                    tmp_file.is_symlink() and tmp_file.is_dir()
                ) or tmp_file.suffix == ".h5":
                    tmp_file.unlink(True)

        return True
