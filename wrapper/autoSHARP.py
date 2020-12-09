import os
import logging
import zocalo
import py
import procrunner
import shutil
from argparse import Namespace

from jinja2.environment import Environment
from jinja2.loaders import PackageLoader
from jinja2.exceptions import UndefinedError
from dlstbx.util import big_ep_helpers
from dlstbx.util import processing_stats
from dlstbx.util.symlink import create_parent_symlink

logger = logging.getLogger("dlstbx.wrap.autoSHARP")


class autoSHARPWrapper(zocalo.wrapper.BaseWrapper):
    def setup_autosharp_jobs(self, working_directory, results_directory):
        """Setup input directory to run autoSHARP."""

        self.msg._wd = os.path.join(self.msg._wd, "autoSHARP")
        self.msg._results_wd = os.path.join(self.msg._results_wd, "autoSHARP")
        os.symlink(working_directory.join("autoSHARP"), self.msg._wd)
        os.symlink(results_directory.join("autoSHARP"), self.msg._results_wd)

        big_ep_helpers.write_sequence_file(self.msg, working_directory.strpath)

        try:
            self.msg.enableArpWarp = self.msg.resolution < 2.5
        except Exception:
            self.msg.enableArpWarp = False

        if hasattr(self.msg, "spacegroup"):
            self.msg.spacegroup = big_ep_helpers.spacegroup_short(
                self.msg.spacegroup, logger
            )

        shutil.copyfile(
            self.msg.hklin, working_directory.join(os.path.basename(self.msg.hklin))
        )

        return self.msg

    def get_autosharp_model_files(self, working_directory):

        parse_value = lambda v: v.split("=")[1][1:-2]

        try:
            with open(os.path.join(self.msg._wd, ".autoSHARP"), "r") as f:
                lines = f.readlines()
                for mtz_line, pdb_line in zip(lines[:0:-1], lines[-2::-1]):
                    if (
                        "autoSHARP_modelmtz=" in mtz_line
                        and "autoSHARP_model=" in pdb_line
                    ):
                        pdb_filename = parse_value(pdb_line).replace(
                            os.path.join(working_directory, "autoSHARP"), self.msg._wd
                        )
                        mtz_filename = parse_value(mtz_line).replace(
                            os.path.join(working_directory, "autoSHARP"), self.msg._wd
                        )
                        mdl_dict = {
                            "pdb": pdb_filename,
                            "mtz": mtz_filename,
                            "pipeline": "autoSHARP",
                            "map": "",
                            "mapcc": 0.0,
                            "mapcc_dmin": 0.0,
                        }
                        if "LJS" in os.path.basename(mdl_dict["mtz"]):
                            mdl_dict.update(
                                {
                                    "fwt": "parrot.F_phi.F",
                                    "phwt": "parrot.F_phi.phi",
                                    "fom": None,
                                }
                            )
                        else:
                            mdl_dict.update({"fwt": "FWT", "phwt": "PHWT", "fom": None})
                        try:
                            mdl_dict.update(
                                processing_stats.get_pdb_chain_stats(
                                    mdl_dict["pdb"], logger
                                )
                            )

                            (
                                map_filename,
                                mapcc,
                                mapcc_dmin,
                            ) = processing_stats.get_mapfile_stats(
                                self.msg._wd, mdl_dict, logger
                            )
                            mdl_dict["map"] = map_filename
                            mdl_dict["mapcc"] = mapcc
                            mdl_dict["mapcc_dmin"] = mapcc_dmin

                        except Exception:
                            logger.exception("autoSHARP results parsing error")
                        self.msg.model = mdl_dict
                        big_ep_helpers.ispyb_write_model_json(self.msg, logger)
                        return self.msg
                logger.error("Cannot find record with autoSHARP output files")
                return None
        except IOError:
            logger.exception("Cannot find .autoSHARP results file")
            return None

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
            create_parent_symlink(
                working_directory.strpath, f"autoSHARP-{ppl}", levels=1
            )

        try:
            self.setup_autosharp_jobs(working_directory, results_directory)
        except Exception:
            logger.exception("Error configuring autoSHARP jobs")
            return False

        tmpl_env = Environment(
            loader=PackageLoader("dlstbx.util.big_ep", "big_ep_templates")
        )
        autosharp_template = tmpl_env.get_template("autoSHARP.sh")
        autosharp_script = working_directory.join("start_autoSHARP.sh")
        with open(autosharp_script, "w") as fp:
            try:
                autosharp_input = autosharp_template.render(vars(self.msg))
            except UndefinedError:
                logger.exception("Error rendering autoSHARP script template")
                return False
            fp.write(autosharp_input)

        result = procrunner.run(
            ["sh", autosharp_script.strpath],
            timeout=params.get("timeout"),
            working_directory=working_directory,
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("runtime: %s", result["runtime"])

        # Just log exit state of the program and try to read any
        # intermediate models in case of failure/timeout
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
        try:
            self.get_autosharp_model_files(working_directory.strpath)
        except Exception:
            if success:
                logger.exception("Error reading autoSHARP model files")
            return False

        if "devel" not in params:
            if params.get("results_directory"):
                big_ep_helpers.copy_results(
                    working_directory.strpath, results_directory.strpath, logger
                )
                if params.get("create_symlink"):
                    create_parent_symlink(
                        results_directory.strpath, f"autoSHARP-{ppl}", levels=1
                    )
                return big_ep_helpers.send_results_to_ispyb(
                    self.msg._results_wd, self.record_result_individual_file, logger
                )
            else:
                logger.debug("Result directory not specified")

        return True
