import os
import logging
import zocalo
import py
import procrunner
from argparse import Namespace

from jinja2.environment import Environment
from jinja2.loaders import PackageLoader
from jinja2.exceptions import UndefinedError
from dlstbx.util.big_ep_helpers import (
    setup_autosharp_jobs,
    copy_results,
    send_results_to_ispyb,
    get_autosharp_model_files,
)
from dlstbx.util.symlink import create_parent_symlink


logger = logging.getLogger("dlstbx.wrap.autoSHARP")


class autoSHARPWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        self.recwrap.environment.update(params["ispyb_parameters"])

        # Collect parameters from payload and check them
        msg = Namespace(**params["msg"])

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])

        # Create working directory with symbolic link
        ppl = params["create_symlink"].replace("/", "-")
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            create_parent_symlink(working_directory.strpath, f"autoSHARP-{ppl}")

        try:
            setup_autosharp_jobs(msg, working_directory, results_directory, logger)
        except Exception:
            logger.exception("Error configuring autoSHARP jobs")
            return False

        tmpl_env = Environment(
            loader=PackageLoader("dlstbx.util.big_ep", "big_ep_templates")
        )
        autosharp_template = tmpl_env.get_template("autoSHARP.sh")
        autosharp_script = os.path.join(msg._wd, "run_autoSHARP.sh")
        with open(autosharp_script, "w") as fp:
            try:
                autosharp_input = autosharp_template.render(vars(msg))
            except UndefinedError:
                logger.exception("Error rendering autoSHARP script template")
                return False
            fp.write(autosharp_input)

        result = procrunner.run(
            ["sh", autosharp_script],
            timeout=params.get("timeout"),
            working_directory=msg._wd,
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("runtime: %s", result["runtime"])
        success = not result["exitcode"] and not result["timeout"]
        if success:
            logger.info("autoSHARP successful, took %.1f seconds", result["runtime"])
        else:
            logger.info(
                "autoSHARP failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            logger.debug(result["stdout"])
            logger.debug(result["stderr"])
            return False
        try:
            get_autosharp_model_files(msg, str(working_directory), logger)
        except Exception:
            logger.exception("Error reading autoSHARP model files")
            return False

        if "devel" not in params:
            if params.get("results_directory"):
                copy_results(
                    working_directory.strpath, results_directory.strpath, logger
                )
                if params.get("create_symlink"):
                    create_parent_symlink(results_directory.strpath, f"autoSHARP-{ppl}")
                return send_results_to_ispyb(
                    msg._results_wd, self.record_result_individual_file, logger
                )
            else:
                logger.debug("Result directory not specified")

        return True
