import logging
import zocalo
import procrunner
import py
from argparse import Namespace
import os

from jinja2.environment import Environment
from jinja2.loaders import PackageLoader
from jinja2.exceptions import UndefinedError

from dlstbx.util.big_ep_helpers import (
    setup_pointless_jobs,
    setup_crank2_jobs,
    get_crank2_model_files,
    copy_results,
    send_results_to_ispyb,
)

logger = logging.getLogger("dlstbx.wrap.crank2")


class Crank2Wrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        self.recwrap.environment.update(params["ispyb_parameters"])

        # Collect parameters from payload and check them
        payload = self.recwrap.payload
        assert payload is not None, "Could not find payload"
        msg = Namespace(**payload)

        working_directory = py.path.local(params["working_directory"])
        ispyb_working_directory = py.path.local(params["ispyb_working_directory"])
        ispyb_results_directory = py.path.local(params["ispyb_results_directory"])

        # Create working directory with symbolic link
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            big_ep_path = ispyb_working_directory.join("..", "big_ep")
            big_ep_path.ensure(dir=True)
            try:
                symlink_path = big_ep_path.join(msg.datetime_stamp)
                symlink_path.mksymlinkto(ispyb_working_directory.join("big_ep"))
            except py.error.EEXIST:
                logger.debug("Symlink %s already exists", symlink_path.strpath)

        # Create big_ep directory to update status in Synchweb
        if "devel" not in params:
            ispyb_results_directory.ensure(dir=True)
            big_ep_path = ispyb_results_directory.join("..", "big_ep")
            big_ep_path.ensure(dir=True)
            if params.get("create_symlink"):
                symlink_path = big_ep_path.join(msg.datetime_stamp)
                try:
                    symlink_path.mksymlinkto(ispyb_results_directory.join("big_ep"))
                except py.error.EEXIST:
                    logger.debug("Symlink %s already exists", symlink_path.strpath)

        try:
            setup_pointless_jobs(msg)
        except Exception:
            logger.exception("Error configuring pointless jobs")
            return False
        tmpl_env = Environment(
            loader=PackageLoader("dlstbx.util.big_ep", "big_ep_templates")
        )
        pointless_template = tmpl_env.get_template("pointless.sh")
        pointless_script = os.path.join(msg._wd, "run_pointless.sh")
        with open(pointless_script, "w") as fp:
            try:
                pointless_input = pointless_template.render(msg.__dict__)
            except UndefinedError:
                logger.exception("Error rendering pointless script template")
                return False
            fp.write(pointless_input)

        result = procrunner.run(
            ["sh", pointless_script],
            timeout=params.get("timeout"),
            working_directory=msg._wd,
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("runtime: %s", result["runtime"])
        success = not result["exitcode"] and not result["timeout"]
        if success:
            logger.info("pointless successful, took %.1f seconds", result["runtime"])
        else:
            logger.info(
                "pointless failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            logger.debug(result["stdout"])
            logger.debug(result["stderr"])

        try:
            setup_crank2_jobs(msg)
        except Exception:
            logger.exception("Error configuring crank2 jobs")
            return False
        crank2_template = tmpl_env.get_template("crank2.sh")
        crank2_script = os.path.join(msg._wd, "run_crank2.sh")
        with open(crank2_script, "w") as fp:
            try:
                crank2_input = crank2_template.render(msg.__dict__)
            except UndefinedError:
                logger.exception("Error rendering crank2 script template")
                return False
            fp.write(crank2_input)

        result = procrunner.run(
            ["sh", crank2_script],
            timeout=params.get("timeout"),
            working_directory=msg._wd,
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("runtime: %s", result["runtime"])
        success = not result["exitcode"] and not result["timeout"]
        if success:
            logger.info("crank2 successful, took %.1f seconds", result["runtime"])
        else:
            logger.info(
                "crank2 failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            logger.debug(result["stdout"])
            logger.debug(result["stderr"])

        try:
            get_crank2_model_files(msg, logger)
        except Exception:
            logger.exception("Error reading crank2 results")
            return False

        if params.get("results_directory"):
            copy_results(msg._wd, msg._results_wd, logger)
            return send_results_to_ispyb(
                msg._results_wd, self.record_result_individual_file, logger
            )
        else:
            logger.debug("Result directory not specified")

        return True
