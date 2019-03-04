from __future__ import absolute_import, division, print_function

import logging
import os
import py

import procrunner
import zocalo.wrapper

logger = logging.getLogger("dlstbx.wrap.mosflm_strategy")


class MosflmStrategyWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        cwd = os.path.abspath(os.curdir)

        working_directory = os.path.abspath(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])
        logger.info("working_directory: %s" % working_directory)
        if not os.path.exists(working_directory):
            os.makedirs(working_directory)
        os.chdir(working_directory)

        # Set SynchWeb to swirl
        results_directory.join("strategy_native.log").ensure()

        if params["image_pattern"].endswith(".h5"):
            self.snowflake2cbf()

        image_directory = params["image_directory"]
        image_pattern = params["image_pattern"]
        image_first = int(params["image_first"])
        image_file_name = os.path.join(image_directory, image_pattern % image_first)
        commands = ["som.strategy", image_file_name]
        space_group = params.get("spacegroup")
        if space_group is not None:
            commands.append(space_group)
        logger.info("command: %s", " ".join(commands))
        result = procrunner.run(commands, timeout=params.get("timeout", 3600))
        if result["exitcode"]:
            logger.info("exitcode: %s", result["exitcode"])
            logger.info(result["stdout"])
            logger.info(result["stderr"])
        logger.info("timeout: %s", result["timeout"])
        logger.info("runtime: %s", result["runtime"])

        if (
            not py.path.local(working_directory).join("strategy_native.log").check()
            or py.path.local(working_directory).join("strategy_native.log").size() == 0
        ):
            result["exitcode"] = 1
            py.path.local(working_directory).join("strategy_native.log").write(
                "failed to determine strategy"
            )

        if py.path.local(working_directory).join("strategy.dat").check():
            # insert results into database
            commands = [
                "/dls_sw/apps/mx-scripts/auto-edna/insertMosflmStrategies1.sh",
                params["dcid"],
                "strategy.dat",
            ]
            logger.info("command: %s", " ".join(commands))
            insertresult = procrunner.run(commands, timeout=params.get("timeout", 3600))
            if insertresult["exitcode"]:
                logger.info("exitcode: %s", insertresult["exitcode"])
                logger.info(insertresult["stdout"])
                logger.info(insertresult["stderr"])
            logger.info("timeout: %s", insertresult["timeout"])
            logger.info("runtime: %s", insertresult["runtime"])

        beamline = params["beamline"]
        if not result["exitcode"] and beamline in ("i03", "i04"):
            result = self.run_xoalign(
                os.path.join(working_directory, "mosflm_index.mat")
            )

        # copy output files to result directory
        logger.info(
            "Copying results from %s to %s"
            % (working_directory, results_directory.strpath)
        )
        for f in py.path.local(working_directory).listdir():
            if not f.basename.startswith("."):
                f.copy(results_directory)

        return result["exitcode"] == 0

    def snowflake2cbf(self):
        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = py.path.local(params["working_directory"])
        tmpdir = working_directory.join("image-tmp")
        tmpdir.ensure(dir=True)
        master_h5 = os.path.join(params["image_directory"], params["image_pattern"])
        prefix = params["image_pattern"].split("master.h5")[0]
        params["image_pattern"] = prefix + "%04d.cbf"
        logger.info("Image pattern: %s", params["image_pattern"])
        logger.info(
            "Converting %s to %s" % (master_h5, tmpdir.join(params["image_pattern"]))
        )
        result = procrunner.run(
            ["dlstbx.snowflake2cbf", master_h5, params["image_pattern"]],
            working_directory=tmpdir.strpath,
            timeout=params.get("timeout", 3600),
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("timeout: %s", result["timeout"])
        logger.info("time_start: %s", result["time_start"])
        logger.info("time_end: %s", result["time_end"])
        logger.info("runtime: %s", result["runtime"])
        logger.info("exitcode: %s", result["exitcode"])
        params["orig_image_directory"] = params["image_directory"]
        params["image_directory"] = tmpdir.strpath

    def run_xoalign(self, mosflm_index_mat):
        print(mosflm_index_mat)
        assert os.path.exists(mosflm_index_mat)
        params = self.recwrap.recipe_step["job_parameters"]
        chi = params.get("chi")
        kappa = params.get("kappa")
        omega = params.get("omega")
        phi = params.get("phi")
        if kappa != "None":
            datum = "-D %s,%s,%s" % (phi, kappa, omega)
        elif chi != "None":
            datum = "-D %s,%s,%s" % (phi, chi, omega)
        else:
            datum = ""
        xoalign_py = "/dls_sw/apps/xdsme/graemewinter-xdsme/bin/Linux_i586/XOalign.py"
        commands = [xoalign_py, datum, mosflm_index_mat]
        logger.info("command: %s", " ".join(commands))
        result = procrunner.run(
            commands,
            timeout=params.get("timeout", 3600),
            environment_override={
                "XOALIGN_CALIB": "/dls_sw/%s/etc/xoalign_config.py" % params["beamline"]
            },
        )
        logger.info("timeout: %s", result["timeout"])
        logger.info("runtime: %s", result["runtime"])
        logger.info("exitcode: %s", result["exitcode"])
        logger.debug(result["stdout"])
        logger.debug(result["stderr"])

        with open("XOalign.log", "wb") as f:
            f.write(result["stdout"])
        self.insertXOalignStrategies(params["dcid"], "XOalign.log")
        return result

    def insertXOalignStrategies(self, dcid, xoalign_log):
        assert os.path.isfile(xoalign_log)
        with open(xoalign_log, "rb") as f:
            smargon = False
            found_solutions = False

            for line in f.readlines():
                if "Independent Solutions" in line:
                    found_solutions = True
                    if "SmarGon" in line:
                        smargon = True
                    continue

                if not found_solutions:
                    continue

                kappa = None
                chi = None
                phi = None
                tokens = line.split()
                if len(tokens) < 4:
                    continue

                solution_id = int(tokens[0])
                angles = [float(t) for t in tokens[1:3]]
                if smargon:
                    chi, phi = angles
                else:
                    kappa, phi = angles
                settings_str = " ".join(tokens[3:]).replace("'", "")
                self.send_alignment_result_to_ispyb(
                    dcid,
                    "XOalign",
                    settings_str,
                    "XOalign %i" % solution_id,
                    chi=chi,
                    kappa=kappa,
                    phi=phi,
                )

    def send_alignment_result_to_ispyb(
        self, dcid, program, comments, short_comments, chi=None, kappa=None, phi=None
    ):

        assert dcid > 0, "Invalid data collection ID given."
        assert [chi, kappa].count(None) == 1
        assert phi is not None
        if kappa is not None and kappa < 0:
            return  # only insert strategies with positive kappa
        if chi is not None and (chi < 0 or chi > 45):
            return  # only insert strategies with 0 < chi > 45
        if phi < 0:
            phi += 360  # make phi always positive
        if kappa is not None:
            kappa = "%.2f" % kappa
        elif chi is not None:
            chi = "%.2f" % chi
        phi = "%.2f" % phi

        result = {
            "dataCollectionId": dcid,
            "program": program,
            "shortComments": short_comments,
            "comments": comments,
            "phi": phi,
        }
        if kappa is not None:
            result["kappa"] = kappa
        elif chi is not None:
            result["chi"] = chi

        logger.debug("Inserting alignment result into ISPyB: %s" % str(result))
        self.recwrap.send_to("alignment-result", result)
