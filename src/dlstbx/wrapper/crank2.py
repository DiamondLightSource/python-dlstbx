import logging
import zocalo
import procrunner
import py
from argparse import Namespace
import os

from jinja2.environment import Environment
from jinja2.loaders import PackageLoader
from jinja2.exceptions import UndefinedError

from dlstbx.util import big_ep_helpers
from dlstbx.util import processing_stats
from dlstbx.util.symlink import create_parent_symlink

logger = logging.getLogger("dlstbx.wrap.crank2")


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

        big_ep_helpers.write_sequence_file(self.msg)

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
        try:
            mdl_dict.update(
                processing_stats.get_pdb_chain_stats(mdl_dict["pdb"], logger)
            )

            (map_filename, mapcc, mapcc_dmin) = processing_stats.get_mapfile_stats(
                self.msg._wd, mdl_dict, logger
            )
            if map_filename:
                mdl_dict["map"] = map_filename
                mdl_dict["mapcc"] = mapcc
                mdl_dict["mapcc_dmin"] = mapcc_dmin

            self.msg.model = mdl_dict
            big_ep_helpers.ispyb_write_model_json(self.msg, logger)

        except Exception:
            logger.info("Cannot process crank2 results files")

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
        tmpl_env = Environment(
            loader=PackageLoader("dlstbx.util.big_ep", "big_ep_templates")
        )
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
        try:
            self.get_crank2_model_files()
        except Exception:
            if success:
                logger.exception("Error reading crank2 results")
            return False

        if "devel" not in params:
            if params.get("results_directory"):
                big_ep_helpers.copy_results(
                    working_directory.strpath, results_directory.strpath, logger
                )
                if params.get("create_symlink"):
                    create_parent_symlink(
                        results_directory.strpath, f"crank2-{ppl}", levels=1
                    )
                return big_ep_helpers.send_results_to_ispyb(
                    self.msg._results_wd, self.record_result_individual_file, logger
                )
            else:
                logger.debug("Result directory not specified")

        return True
