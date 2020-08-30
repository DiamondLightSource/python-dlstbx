import logging
import os
import py

import dlstbx.util.symlink
import procrunner
import zocalo.wrapper
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
    def go_fast_ep(self, params):
        """Decide whether to run fast_ep or not based on the completeness, dI/s(dI) and
        resolution of actual data."""

        from iotbx.reflection_file_reader import any_reflection_file

        if "go_fast_ep" not in params:
            logger.info("go_fast_ep settings not available")
            return False

        thres_d_min = params["go_fast_ep"].get("d_min", -1)

        def check_thresholds(data, threshold):
            thres_completeness = threshold.get("completeness", -1)
            thres_dIsigdI = threshold.get("dI/sigdI", -1)

            differences = data.anomalous_differences()
            dIsigdI = sum(abs(differences.data())) / sum(differences.sigmas())
            completeness = data.completeness()
            if completeness < thres_completeness:
                logger.info(
                    "Data completeness %.2f below threshold value %.2f. Aborting."
                    % (completeness, thres_completeness)
                )
                return True
            if dIsigdI < thres_dIsigdI:
                logger.info(
                    "Data dI/s(dI) %.2f below threshold value %.2f. Aborting."
                    % (dIsigdI, thres_dIsigdI)
                )
                return True
            logger.info(
                "Data completeness: %.2f  threshold: %.2f"
                % (completeness, thres_completeness)
            )
            logger.info(
                "Data dI/s(dI): %.2f  threshold: %.2f" % (dIsigdI, thres_dIsigdI)
            )
            return False

        hkl_file = any_reflection_file(params["fast_ep"]["data"])
        mas = hkl_file.as_miller_arrays()
        try:
            all_data = next(m for m in mas if m.anomalous_flag())
        except StopIteration:
            logger.exception(
                "No anomalous data found in %s" % params["fast_ep"]["data"]
            )
            return True
        if all_data.d_min() > thres_d_min:
            select_data = all_data
            res = check_thresholds(select_data, params["go_fast_ep"].get("low_res", {}))
        else:
            select_data = all_data.resolution_filter(d_min=thres_d_min)
            res = check_thresholds(
                select_data, params["go_fast_ep"].get("high_res", {})
            )
        return res

    def construct_commandline(self, params):
        """Construct fast_ep command line.
        Takes job parameter dictionary, returns array."""

        command = ["fast_ep"]
        for param, value in params["fast_ep"].items():
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
        success = not result["exitcode"] and not result["timeout"]
        if success:
            logger.info(
                "phasing2ispyb successful, took %.1f seconds", result["runtime"]
            )
        else:
            logger.info(
                "phasing2ispyb failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            logger.debug(result["stdout"])
            logger.debug(result["stderr"])
        return success

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
            if (
                params["ispyb_parameters"].get("check_go_fast_ep")
                or "go_fast_ep" in params
            ) and self.go_fast_ep(params):
                logger.info("Skipping fast_ep (go_fast_ep == No)")
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
        fast_ep_script = working_directory.join("run_fast_ep.sh")
        with fast_ep_script.open("w") as fp:
            fp.writelines(
                [
                    ". /etc/profile.d/modules.sh\n",
                    "module purge\n",
                    "module load fast_ep\n",
                    " ".join(command),
                ]
            )
        result = procrunner.run(
            ["sh", fast_ep_script.strpath],
            timeout=params.get("timeout"),
            working_directory=working_directory,
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("runtime: %s", result["runtime"])
        success = (
            not result["exitcode"]
            and not result["timeout"]
            and not working_directory.join("fast_ep.error").check()
        )
        if success:
            logger.info("fast_ep successful, took %.1f seconds", result["runtime"])
        else:
            logger.info(
                "fast_ep failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
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
            logger.warning(
                "fast_ep failed. Results file %s unavailable", fast_ep_data_json.strpath
            )
            return False

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
                elif success:
                    logger.error(
                        "Expected output file does not exist: %s" % xml_file.strpath
                    )
                else:
                    logger.info(
                        "fast_ep failed, no .xml output, thus not reporting to ISPyB"
                    )
                    return False
        except NameError:
            logger.info(
                "Copying fast_ep results ignored. Results directory unavailable."
            )

        return success
