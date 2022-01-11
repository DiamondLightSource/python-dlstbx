from __future__ import annotations

import logging
import os
from argparse import Namespace

import procrunner
import py
import zocalo
from jinja2.environment import Environment
from jinja2.exceptions import UndefinedError
from jinja2.loaders import PackageLoader

from dlstbx.util.big_ep_helpers import (
    copy_results,
    ispyb_write_model_json,
    send_results_to_ispyb,
    write_coot_script,
    write_sequence_file,
)
from dlstbx.util.processing_stats import get_model_data
from dlstbx.util.symlink import create_parent_symlink

logger = logging.getLogger("dlstbx.wrap.crank2")

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
}


class Crank2Wrapper(zocalo.wrapper.BaseWrapper):
    def setup_pointless_jobs(self, working_directory):
        """Update spacegroup in the input mtz file"""

        self.msg.input_hkl = self.msg.hklin
        (_, filext) = os.path.split(self.msg.hklin)
        (filename, ext) = os.path.splitext(filext)
        self.msg.hklin = working_directory.join(
            "".join([filename, self.msg.spacegroup, ext])
        ).strpath

    def setup_crank2_jobs(self, working_directory, results_directory):
        """Setup directory to run Crank2 pipeline"""

        self.msg._wd = os.path.join(self.msg._root_wd, "crank2")
        self.msg._results_wd = os.path.join(self.msg._results_wd, "crank2")
        os.symlink(working_directory, self.msg._wd)
        os.symlink(results_directory, self.msg._results_wd)

        try:
            self.msg.enableArpWarp = self.msg.resolution < 2.5
        except Exception:
            self.msg.enableArpWarp = False

        write_sequence_file(self.msg)

    def get_crank2_model_files(self):

        ref_pth = os.path.join(self.msg._wd, "crank2", "5-comb_phdmmb", "ref")
        dmfull_pth = os.path.join(
            self.msg._wd, "crank2", "5-comb_phdmmb", "dmfull", "ref"
        )
        if os.path.isdir(ref_pth):
            mdl_dict = {
                "pdb": os.path.join(ref_pth, "sepsubstrprot", "part.pdb"),
                "mtz": os.path.join(ref_pth, "refmac", "REFMAC5.mtz"),
                "pipeline": "Crank2",
            }
        elif os.path.isdir(dmfull_pth):
            mdl_dict = {
                "pdb": os.path.join(dmfull_pth, "sepsubstrprot", "part.pdb"),
                "mtz": os.path.join(dmfull_pth, "REFMAC5.mtz"),
                "pipeline": "Crank2",
            }
        else:
            return

        mdl_dict.update({"fwt": "REFM_FWT", "phwt": "REFM_PHWT", "fom": None})
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

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])

        # Create working directory with symbolic link
        ppl = params["create_symlink"].replace("/", "-")
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            create_parent_symlink(working_directory.strpath, f"crank2-{ppl}", levels=1)

        try:
            self.setup_pointless_jobs(working_directory)
        except Exception:
            logger.exception("Error configuring pointless jobs")
            return False
        tmpl_env = Environment(loader=PackageLoader("dlstbx.util", "big_ep_templates"))
        pointless_template = tmpl_env.get_template("pointless.sh")
        pointless_script = working_directory.join("run_pointless.sh")
        with open(pointless_script, "w") as fp:
            try:
                pointless_input = pointless_template.render(self.msg.__dict__)
            except UndefinedError:
                logger.exception("Error rendering pointless script template")
                return False
            fp.write(pointless_input)

        result = procrunner.run(
            ["sh", pointless_script.strpath],
            timeout=params.get("timeout"),
            working_directory=working_directory,
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
            self.setup_crank2_jobs(working_directory, results_directory)
        except Exception:
            logger.exception("Error configuring crank2 jobs")
            return False
        crank2_template = tmpl_env.get_template("crank2.sh")
        crank2_script = os.path.join(self.msg._wd, "run_crank2.sh")
        with open(crank2_script, "w") as fp:
            try:
                crank2_input = crank2_template.render(self.msg.__dict__)
            except UndefinedError:
                logger.exception("Error rendering crank2 script template")
                return False
            fp.write(crank2_input)

        result = procrunner.run(
            ["sh", crank2_script],
            timeout=params.get("timeout"),
            working_directory=self.msg._wd,
            environment_override=clean_environment,
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("runtime: %s", result["runtime"])

        # Just log exit state of the program and try to read any
        # intermediate models in case of failure/timeout
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

        mdl_dict = self.get_crank2_model_files()
        if mdl_dict is None:
            logger.info("Cannot process crank2 results")
        else:
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
                        results_directory.strpath, f"crank2-{ppl}", levels=1
                    )
                if mdl_dict:
                    return send_results_to_ispyb(
                        params.get("results_directory"),
                        params.get("log_files"),
                        self.record_result_individual_file,
                    )
            else:
                logger.debug("Result directory not specified")

        if mdl_dict is None:
            return False
        return True
