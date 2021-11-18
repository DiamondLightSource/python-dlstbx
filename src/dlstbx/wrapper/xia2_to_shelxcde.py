import glob
import logging
import os

import procrunner
import py
import zocalo.wrapper

import dlstbx.util.symlink
from dlstbx.util.shelxc import parse_shelxc_logs

logger = logging.getLogger("zocalo.wrap.xia2.to_shelxcde")


class Xia2toShelxcdeWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = py.path.local(os.path.join(params["working_directory"]))
        try:
            results_directory = py.path.local(
                os.path.realpath(os.path.join(params["results_directory"]))
            )
        except KeyError:
            logger.debug("Result directory not specified")

        # Create working directory with symbolic link
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            try:
                levels = params["levels_symlink"]
                dlstbx.util.symlink.create_parent_symlink(
                    working_directory.strpath, params["create_symlink"], levels=levels
                )
            except KeyError:
                dlstbx.util.symlink.create_parent_symlink(
                    working_directory.strpath, params["create_symlink"]
                )

        data_files = sorted(glob.glob(params["data"]))
        if not data_files:
            logger.error(
                "Could not find data files matching %s to process", params["data"]
            )
            return False

        file_list = []
        if len(data_files) > 1:
            for tag, data_file in zip(
                ["--peak", "--infl", "--hrem", "--lrem"], data_files
            ):
                file_list.extend([tag, data_file])
        else:
            file_list = ["--sad"] + data_files
        command = ["xia2.to_shelxcde"] + file_list + ["shelxc"]
        logger.info("Generating SHELXC .ins file")
        logger.info("command: %s", " ".join(command))
        result = procrunner.run(
            command,
            timeout=params.get("timeout"),
            working_directory=working_directory.strpath,
        )
        if result["exitcode"] or result["timeout"]:
            logger.info("timeout: %s", result["timeout"])
            logger.info("exitcode: %s", result["exitcode"])
            logger.debug(result["stdout"].decode("latin1"))
            logger.debug(result["stderr"].decode("latin1"))
        logger.info("runtime: %s", result["runtime"])

        command = ["sh", "shelxc.sh"]
        logger.info("Starting SHELXC")
        logger.info("command: %s", " ".join(command))
        result = procrunner.run(
            command,
            timeout=params.get("timeout"),
            working_directory=working_directory.strpath,
        )

        if result["exitcode"] or result["timeout"]:
            logger.info("timeout: %s", result["timeout"])
            logger.info("exitcode: %s", result["exitcode"])
            logger.debug(result["stdout"].decode("latin1"))
            logger.debug(result["stderr"].decode("latin1"))
        logger.info("runtime: %s", result["runtime"])

        if not result["stdout"]:
            logger.debug("SHELXC log is empty")
            return False

        shelxc_log = os.path.join(working_directory.strpath, "results_shelxc.log")
        with open(shelxc_log, "w") as fp:
            fp.write(result["stdout"].decode("latin1"))

        stats = parse_shelxc_logs(result["stdout"].decode("latin1"), logger)
        if not stats:
            logger.debug("Cannot process SHELXC data. Aborting.")
            return False
        self.recwrap.send_to("downstream", stats)

        # Create results directory and symlink if they don't already exist
        try:
            logger.info("Copying SHELXC results to %s", results_directory.strpath)
            results_directory.ensure(dir=True)
            if params.get("create_symlink"):
                try:
                    levels = params["levels_symlink"]
                    dlstbx.util.symlink.create_parent_symlink(
                        results_directory.strpath,
                        params["create_symlink"],
                        levels=levels,
                    )
                except KeyError:
                    dlstbx.util.symlink.create_parent_symlink(
                        results_directory.strpath, params["create_symlink"]
                    )
            for f in working_directory.listdir():
                if f.ext in [".log", ".hkl", ".sh", ".ins", ".cif"]:
                    f.copy(results_directory)
        except NameError:
            logger.debug(
                "Ignore copying SHELXC results. Results directory not specified."
            )
        return result["exitcode"] == 0
