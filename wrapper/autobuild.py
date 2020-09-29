import logging
import zocalo
import procrunner

from jinja2.environment import Environment
from jinja2.loaders import PackageLoader
from jinja2.exceptions import UndefinedError

import py
from dlstbx.util.symlink import create_parent_symlink
from dlstbx.util.big_ep_helpers import (
    copy_results,
    send_results_to_ispyb,
    setup_autosol_jobs,
    get_autobuild_model_files,
)
from argparse import Namespace
import os


logger = logging.getLogger("dlstbx.wrap.autobuild")


class AutoBuildWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        self.recwrap.environment.update(params["ispyb_parameters"])

        # Collect parameters from payload and check them
        msg = Namespace(**params["msg"])
        msg.workingdir = self.recwrap.recipe_step["parameters"]["workingdir"]

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])

        # Create working directory with symbolic link
        ppl = params["create_symlink"].replace("/", "-")
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            create_parent_symlink(working_directory.strpath, f"AutoBuild-{ppl}")

        try:
            setup_autosol_jobs(msg, working_directory, results_directory)
        except Exception:
            logger.exception("Error configuring autoSol jobs")
            return False

        tmpl_env = Environment(
            loader=PackageLoader("dlstbx.util.big_ep", "big_ep_templates")
        )
        autosol_template = tmpl_env.get_template("autosol.sh")
        autosol_script = os.path.join(msg._wd, "run_autosol.sh")
        with open(autosol_script, "w") as fp:
            try:
                autosol_input = autosol_template.render(msg.__dict__)
            except UndefinedError:
                logger.exception("Error rendering AutoSol script template")
                return False
            fp.write(autosol_input)

        result = procrunner.run(
            ["sh", autosol_script],
            timeout=params.get("timeout"),
            working_directory=msg._wd,
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("runtime: %s", result["runtime"])
        success = not result["exitcode"] and not result["timeout"]
        if success:
            logger.info("AutoSol successful, took %.1f seconds", result["runtime"])
        else:
            logger.info(
                "AutoSol failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            logger.debug(result["stdout"])
            logger.debug(result["stderr"])

        autobuild_template = tmpl_env.get_template("autobuild.sh")
        autobuild_script = os.path.join(msg._wd, "run_autobuild.sh")
        with open(autobuild_script, "w") as fp:
            try:
                autobuild_input = autobuild_template.render(msg.__dict__)
            except UndefinedError:
                logger.exception("Error rendering AutoBuild script template")
                return False
            fp.write(autobuild_input)

        result = procrunner.run(
            ["sh", autobuild_script],
            timeout=params.get("timeout"),
            working_directory=msg._wd,
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("runtime: %s", result["runtime"])
        success = not result["exitcode"] and not result["timeout"]
        if success:
            logger.info("AutoBuild successful, took %.1f seconds", result["runtime"])
        else:
            logger.info(
                "AutoBuild failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            logger.debug(result["stdout"])
            logger.debug(result["stderr"])

        try:
            get_autobuild_model_files(msg, logger)
        except Exception:
            logger.exception("Error reading AutoBuild results")
            return False

        if "devel" not in params:
            if params.get("results_directory"):
                copy_results(
                    working_directory.strpath, results_directory.strpath, logger
                )
                if params.get("create_symlink"):
                    create_parent_symlink(results_directory.strpath, f"AutoBuild-{ppl}")
                return send_results_to_ispyb(
                    msg._results_wd, self.record_result_individual_file, logger
                )
            else:
                logger.debug("Result directory not specified")

        return True
