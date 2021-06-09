import os
import logging
import zocalo
import procrunner
from argparse import Namespace

from jinja2.environment import Environment
from jinja2.loaders import PackageLoader
from jinja2.exceptions import UndefinedError

import py
from dlstbx.util.symlink import create_parent_symlink
import shutil
from dlstbx.util.processing_stats import get_model_data
from dlstbx.util.big_ep_helpers import (
    ispyb_write_model_json,
    write_coot_script,
    send_results_to_ispyb,
    copy_results,
    write_sequence_file,
)


logger = logging.getLogger("dlstbx.wrap.autobuild")


class AutoBuildWrapper(zocalo.wrapper.BaseWrapper):
    def setup_autosol_jobs(self, working_directory, results_directory):
        """Setup working directory for running Phenix AutoSol pipeline"""

        self.msg._wd = os.path.join(self.msg._wd, "AutoSol")
        self.msg._results_wd = os.path.join(self.msg._results_wd, "AutoSol")
        os.symlink(working_directory, self.msg._wd)
        os.symlink(results_directory, self.msg._results_wd)
        if not os.path.exists(self.msg._wd):
            os.makedirs(self.msg._wd)

        write_sequence_file(self.msg)

        self.msg.autosol_hklin = os.path.join(
            self.msg._wd, os.path.basename(self.msg.hklin)
        )
        shutil.copyfile(self.msg.hklin, self.msg.autosol_hklin)

    def get_autobuild_model_files(self):

        mdl_dict = {
            "pdb": os.path.join(self.msg._wd, "AutoBuild_run_1_", "overall_best.pdb"),
            "mtz": os.path.join(
                self.msg._wd, "AutoBuild_run_1_", "overall_best_denmod_map_coeffs.mtz"
            ),
            "pipeline": "AutoBuild",
            "fwt": "FWT",
            "phwt": "PHWT",
            "fom": None,
        }
        model_data = get_model_data(self.msg._wd, mdl_dict, logger)
        if model_data is None:
            return

        mdl_dict.update(model_data)
        return mdl_dict

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        self.recwrap.environment.update(params["ispyb_parameters"])

        # Collect parameters from payload and check them
        self.msg = Namespace(**params["msg"])
        self.msg.workingdir = self.recwrap.recipe_step["parameters"]["workingdir"]

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])

        # Create working directory with symbolic link
        ppl = params["create_symlink"].replace("/", "-")
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            create_parent_symlink(
                working_directory.strpath, f"AutoBuild-{ppl}", levels=1
            )

        try:
            self.setup_autosol_jobs(working_directory, results_directory)
        except Exception:
            logger.exception("Error configuring autoSol jobs")
            return False

        tmpl_env = Environment(loader=PackageLoader("dlstbx.util", "big_ep_templates"))
        autosol_template = tmpl_env.get_template("autosol.sh")
        autosol_script = os.path.join(self.msg._wd, "run_autosol.sh")
        with open(autosol_script, "w") as fp:
            try:
                autosol_input = autosol_template.render(self.msg.__dict__)
            except UndefinedError:
                logger.exception("Error rendering AutoSol script template")
                return False
            fp.write(autosol_input)

        result = procrunner.run(
            ["sh", autosol_script],
            timeout=params.get("timeout"),
            working_directory=self.msg._wd,
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
        autobuild_script = os.path.join(self.msg._wd, "run_autobuild.sh")
        with open(autobuild_script, "w") as fp:
            try:
                autobuild_input = autobuild_template.render(self.msg.__dict__)
            except UndefinedError:
                logger.exception("Error rendering AutoBuild script template")
                return False
            fp.write(autobuild_input)

        result = procrunner.run(
            ["sh", autobuild_script],
            timeout=params.get("timeout"),
            working_directory=self.msg._wd,
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("runtime: %s", result["runtime"])

        # Just log exit state of the program and try to read any
        # intermediate models in case of failure/timeout
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

        mdl_dict = self.get_autobuild_model_files()
        if mdl_dict is None:
            if success:
                logger.exception("Error reading AutoBuild results")
            else:
                logger.info("Cannot process AutoBuild results")
            return False

        self.msg.model = mdl_dict
        ispyb_write_model_json(self.msg, logger)
        write_coot_script(self.msg._wd, mdl_dict)

        if "devel" not in params:
            if params.get("results_directory"):
                copy_results(
                    working_directory.strpath, results_directory.strpath, logger
                )
                if params.get("create_symlink"):
                    create_parent_symlink(
                        results_directory.strpath, f"AutoBuild-{ppl}", levels=1
                    )
                return send_results_to_ispyb(
                    params.get("results_directory"),
                    params.get("log_files"),
                    self.record_result_individual_file,
                )
            else:
                logger.debug("Result directory not specified")

        return True
