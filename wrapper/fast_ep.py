from __future__ import absolute_import, division, print_function

import logging
import os
import py

import dlstbx.util.symlink
import procrunner
import zocalo.wrapper
import tempfile
import json
from pprint import pformat

logger = logging.getLogger("dlstbx.wrap.fast_ep")

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
}


class FastEPWrapper(zocalo.wrapper.BaseWrapper):
    def check_go_fast_ep(self, params):
        command = ["go_fast_ep", params["fast_ep"]["data"]]
        result = procrunner.run(
            command,
            timeout=params.get("timeout"),
            print_stdout=True,
            print_stderr=True,
            working_directory=params["working_directory"],
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("timeout: %s", result["timeout"])
        logger.info("time_start: %s", result["time_start"])
        logger.info("time_end: %s", result["time_end"])
        logger.info("runtime: %s", result["runtime"])
        logger.info("exitcode: %s", result["exitcode"])
        logger.debug(result["stdout"])
        logger.debug(result["stderr"])

        go_fast_ep = result["stdout"].strip() == "Go"
        if go_fast_ep:
            logger.info("Computer says go for fast_ep :)")
        else:
            logger.info("Computer says no for fast_ep :(")
        return go_fast_ep

    def construct_commandline(self, params):
        """Construct fast_ep command line.
       Takes job parameter dictionary, returns array."""

        command = ["fast_ep"]
        for param, value in params["fast_ep"].iteritems():
            logging.info("Parameter %s: %s" % (param, str(value)))
            if param == "rlims":
                value = ",".join(str(r) for r in value)
            command.append("%s=%s" % (param, value))

        return command

    def send_results_to_ispyb(self, xml_file):
        params = self.recwrap.recipe_step["job_parameters"]
        command = [
            "python",
            "/dls_sw/apps/mx-scripts/dbserver/src/phasing2ispyb.py",
            "-s",
            "sci-serv3",
            "-p",
            "2611",
            "--fix_sgids",
            "-d",
            "-i",
            xml_file,
            "-f",
            params["fast_ep"]["data"],
            "-o",
            os.path.join(params["working_directory"], "fast_ep_ispyb_ids.xml"),
        ]

        result = procrunner.run(
            command,
            timeout=params.get("timeout"),
            print_stdout=True,
            print_stderr=True,
            working_directory=params["working_directory"],
            environment_override=clean_environment,
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("timeout: %s", result["timeout"])
        logger.info("time_start: %s", result["time_start"])
        logger.info("time_end: %s", result["time_end"])
        logger.info("runtime: %s", result["runtime"])
        logger.info("exitcode: %s", result["exitcode"])
        logger.debug(result["stdout"])
        logger.debug(result["stderr"])
        return result["exitcode"] == 0

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = py.path.local(params["working_directory"])
        try:
            results_directory = py.path.local(params["results_directory"])
        except KeyError:
            logger.info("Results directory not specified")

        if "ispyb_parameters" in params:
            if params["ispyb_parameters"].get("data"):
                params["fast_ep"]["data"] = os.path.abspath(
                    params["ispyb_parameters"]["data"]
                )
            if params["ispyb_parameters"].get("check_go_fast_ep"):
                if not self.check_go_fast_ep(params):
                    logger.info("Skipping fast_ep (check_go_fast_ep == No)")
                    return False

        # Create working directory with symbolic link
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory.strpath, params["create_symlink"]
            )

        # Create SynchWeb ticks hack file. This will be overwritten with the real log later.
        # For this we need to create the results directory and symlink immediately.
        try:
            if params.get("synchweb_ticks"):
                logger.debug("Setting SynchWeb status to swirl")
                if params.get("create_symlink"):
                    results_directory.ensure(dir=True)
                    dlstbx.util.symlink.create_parent_symlink(
                        results_directory.strpath, params["create_symlink"]
                    )
                py.path.local(params["synchweb_ticks"]).ensure()
        except NameError:
            logger.info(
                "Setting SynchWeb symlinks ignored. Results directory unavailable."
            )

        command = self.construct_commandline(params)
        try:
            fp = tempfile.NamedTemporaryFile(dir=working_directory.strpath)
            fast_ep_script = os.path.join(
                working_directory.strpath,
                "run_fast_ep_{}.sh".format(os.path.basename(fp.name)),
            )
            fp.close()
            with open(fast_ep_script, "w") as fp:
                fp.writelines(
                    [
                        ". /etc/profile.d/modules.sh\n",
                        "module load fast_ep\n",
                        " ".join(command),
                    ]
                )
        except IOError:
            logger.exception(
                "Could not create fast_ep script file in the working directory"
            )
            return False
        result = procrunner.run(
            ["sh", fast_ep_script],
            timeout=params.get("timeout"),
            print_stdout=False,
            print_stderr=False,
            working_directory=working_directory.strpath,
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("timeout: %s", result["timeout"])
        logger.info("time_start: %s", result["time_start"])
        logger.info("time_end: %s", result["time_end"])
        logger.info("runtime: %s", result["runtime"])
        logger.info("exitcode: %s", result["exitcode"])
        logger.debug(result["stdout"])
        logger.debug(result["stderr"])

        # Send results to topaz for hand determination
        fast_ep_data_json = working_directory.join("fast_ep_data.json")
        if fast_ep_data_json.check():
            with fast_ep_data_json.open("r") as fp:
                fast_ep_data = json.load(fp)
            with working_directory.join("fast_ep.log").open("r") as fp:
                for line in fp:
                    if "Unit cell:" in line:
                        cell_info = tuple(float(v) for v in line.split()[2:])
                        break
            best_sg = fast_ep_data["_spacegroup"][0]
            best_solv = "{0:.2f}".format(fast_ep_data["solv"])
            original_hand = working_directory.join(best_solv, "sad.phs")
            inverted_hand = working_directory.join(best_solv, "sad_i.phs")
            hkl_data = working_directory.join(best_solv, "sad.hkl")
            fa_data = working_directory.join(best_solv, "sad_fa.hkl")
            res_data = working_directory.join(best_solv, "sad_fa.res")
            topaz_data = {
                "original_phase_file": original_hand.strpath,
                "inverse_phase_file": inverted_hand.strpath,
                "hkl_file": hkl_data.strpath,
                "fa_file": fa_data.strpath,
                "res_file": res_data.strpath,
                "space_group": best_sg,
                "cell_info": cell_info,
                "best_solvent": best_solv,
            }
            logger.info("Topaz data: %s", pformat(topaz_data))
            self.recwrap.send_to("topaz", topaz_data)
        else:
            logger.warning("fast_ep results file missing")

        # Create results directory and symlink if they don't already exist
        try:
            results_directory.ensure(dir=True)
            if params.get("create_symlink"):
                dlstbx.util.symlink.create_parent_symlink(
                    results_directory.strpath, params["create_symlink"]
                )

            logger.info("Copying fast_ep results to %s", results_directory.strpath)
            keep_ext = {
                ".cif": "result",
                ".error": "log",
                ".hkl": "result",
                ".html": "log",
                ".ins": "result",
                ".lst": "log",
                ".mtz": "result",
                ".pdb": "result",
                ".png": None,
                ".sca": "result",
                ".sh": None,
                ".xml": False,
            }
            keep = {"fast_ep.log": "log", "shelxc.log": "log"}
            if working_directory.join("fast_ep.error").check():
                result["exitcode"] = 1
            allfiles = []
            for filename in working_directory.listdir():
                filetype = keep_ext.get(filename.ext)
                if filename.basename in keep:
                    filetype = keep[filename.basename]
                if filetype is None:
                    continue
                destination = results_directory.join(filename.basename)
                filename.copy(destination)
                allfiles.append(destination.strpath)
                if filetype:
                    self.record_result_individual_file(
                        {
                            "file_path": destination.dirname,
                            "file_name": destination.basename,
                            "file_type": filetype,
                        }
                    )

            if "xml" in params["fast_ep"]:
                xml_file = working_directory.join(params["fast_ep"]["xml"])
                if xml_file.check():
                    xml_data = working_directory.join(params["fast_ep"]["xml"]).read()
                    logger.info("Sending fast_ep phasing results to ISPyB")
                    xml_file.write(
                        xml_data.replace(
                            working_directory.strpath, results_directory.strpath
                        )
                    )
                    result_ispyb = self.send_results_to_ispyb(xml_file.strpath)
                    if not result_ispyb:
                        logger.error(
                            "Running phasing2ispyb.py script returned non-zero exit code"
                        )
                else:
                    if result["exitcode"]:
                        logger.info(
                            "fast_ep failed, no .xml output, thus not reporting to ISPyB"
                        )
                    else:
                        logger.error(
                            "Expected output file does not exist: %s" % xml_file.strpath
                        )
                    return False
        except NameError:
            logger.info(
                "Copying fast_ep results ignored. Results directory unavailable."
            )

        return result["exitcode"] == 0
