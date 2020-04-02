from __future__ import absolute_import, division, print_function

import glob
import logging
import copy
import os
import shutil

import dlstbx.util.symlink
import procrunner
import py
import zocalo.wrapper
from six.moves import configparser

logger = logging.getLogger("dlstbx.wrap.dimple")


class DimpleWrapper(zocalo.wrapper.BaseWrapper):
    def send_results_to_ispyb(self):
        log_file = self.results_directory.join("dimple.log")
        if not log_file.check():
            logger.error(
                "Can not insert dimple results into ISPyB: dimple.log not found"
            )
            return False
        log = configparser.RawConfigParser()
        log.read(log_file.strpath)

        scaling_id = self.params.get("ispyb_parameters", self.params).get(
            "scaling_id", []
        )
        assert len(scaling_id) == 1, (
            "Exactly one scaling id must be provided: %s" % scaling_id
        )
        scaling_id = scaling_id[0]
        if not str(scaling_id).isdigit():
            logger.error(
                "Can not write results to ISPyB: no scaling ID set (%r)", scaling_id
            )
            return False
        scaling_id = int(scaling_id)
        logger.debug(
            "Inserting dimple phasing results from %s into ISPyB for scaling_id %d",
            self.results_directory.strpath,
            scaling_id,
        )

        ispyb_command_list = []

        starttime = log.get(log.sections()[1], "start_time")
        endtime = log.get(log.sections()[-1], "end_time")
        try:
            msg = " ".join(log.get("find-blobs", "info").split()[:4])
        except configparser.NoSectionError:
            msg = "Unmodelled blobs not found"
        dimple_args = log.get("workflow", "args").split()

        insert_mxmr_run = {
            "ispyb_command": "insert_mxmr_run",
            "store_result": "ispyb_mxmr_run_id",
            "scaling_id": scaling_id,
            "pipeline": "dimple",
            "log_file": log_file.strpath,
            "success": 1,
            "starttime": starttime,
            "endtime": endtime,
            "rfree_start": log.getfloat("refmac5 restr", "ini_free_r"),
            "rfree_end": log.getfloat("refmac5 restr", "free_r"),
            "r_start": log.getfloat("refmac5 restr", "ini_overall_r"),
            "r_end": log.getfloat("refmac5 restr", "overall_r"),
            "message": msg,
            "run_dir": self.results_directory.strpath,
            "input_MTZ_file": dimple_args[0],
            "input_coord_file": dimple_args[1],
            "output_MTZ_file": self.results_directory.join("final.mtz").strpath,
            "output_coord_file": self.results_directory.join("final.pdb").strpath,
            "cmd_line": (
                log.get("workflow", "prog")
                + " "
                + log.get("workflow", "args").replace("\n", " ")
            ),
        }
        ispyb_command_list.append(insert_mxmr_run)

        for n in (1, 2):
            if self.results_directory.join("/blob{}v1.png".format(n)).check():
                insert_mxmr_run_blob = {
                    "ispyb_command": "insert_mxmr_run_blob",
                    "mxmr_run_id": "$ispyb_mxmr_run_id",
                    "view1": "blob{}v1.png".format(n),
                    "view2": "blob{}v2.png".format(n),
                    "view3": "blob{}v3.png".format(n),
                }
                ispyb_command_list.append(insert_mxmr_run_blob)

        logger.debug("Sending %s", str(ispyb_command_list))
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        return True

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.params = self.recwrap.recipe_step["job_parameters"]
        self.working_directory = py.path.local(self.params["working_directory"])
        self.results_directory = py.path.local(self.params["results_directory"])
        self.working_directory.ensure(dir=True)

        mtz = self.params.get("ispyb_parameters", self.params.get("dimple", {})).get(
            "data", []
        )
        if not mtz:
            logger.error("Could not identify on what data to run")
            return False

        assert len(mtz) == 1, "Exactly one data file data file must be provided: %s" % (
            mtz
        )
        mtz = os.path.abspath(mtz[0])
        if not os.path.exists(mtz):
            logger.error("Could not find data file %s to process", mtz)
            return False
        pdb = self.params.get("ispyb_parameters", {}).get("pdb") or self.params[
            "dimple"
        ].get("pdb", [])
        if not pdb:
            logger.error("Not running dimple as no PDB file available")
            return False

        pdb = copy.deepcopy(pdb)  # otherwise we could modify the array in the recipe
        for i, code_or_file in enumerate(pdb):
            if os.path.isfile(code_or_file):
                shutil.copy(code_or_file, self.working_directory.strpath)
                pdb[i] = self.working_directory / os.path.basename(code_or_file)

        command = (
            ["dimple", mtz]
            + pdb
            + [
                self.working_directory,
                # '--dls-naming',
                "--anode",
                "-fpng",
            ]
        )

        if self.params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                self.working_directory.strpath, self.params["create_symlink"]
            )

        # Create SynchWeb ticks hack file. This will be deleted or replaced later.
        # For this we need to create the results directory and its symlink immediately.
        if self.params.get("synchweb_ticks") and self.params.get(
            "ispyb_parameters", {}
        ).get("set_synchweb_status"):
            logger.debug("Setting SynchWeb status to swirl")
            if self.params.get("create_symlink"):
                self.results_directory.ensure(dir=True)
                dlstbx.util.symlink.create_parent_symlink(
                    self.results_directory.strpath, self.params["create_symlink"]
                )
                mtzsymlink = os.path.join(
                    os.path.dirname(mtz), self.params["create_symlink"]
                )
                if not os.path.exists(mtzsymlink):
                    deltapath = os.path.relpath(
                        self.results_directory.strpath, os.path.dirname(mtz)
                    )
                    os.symlink(deltapath, mtzsymlink)
            py.path.local(self.params["synchweb_ticks"]).ensure()

        logger.info("command: %s", " ".join(map(str, command)))
        result = procrunner.run(
            command,
            working_directory=self.working_directory.strpath,
            timeout=self.params.get("timeout"),
        )
        success = not result["exitcode"] and not result["timeout"]
        if success:
            logger.info("dimple successful, took %.1f seconds", result["runtime"])
        else:
            logger.info(
                "dimple failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            logger.debug(result["stdout"].decode("latin1"))
            logger.debug(result["stderr"].decode("latin1"))

        # Hack to workaround dimple returning successful exitcode despite 'Giving up'
        success = b"Giving up" not in result.stdout

        logger.info("Copying DIMPLE results to %s", self.results_directory.strpath)
        self.results_directory.ensure(dir=True)
        if self.params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                self.results_directory.strpath, self.params["create_symlink"]
            )
            mtzsymlink = os.path.join(
                os.path.dirname(mtz), self.params["create_symlink"]
            )
            if not os.path.exists(mtzsymlink):
                deltapath = os.path.relpath(
                    self.results_directory.strpath, os.path.dirname(mtz)
                )
                os.symlink(deltapath, mtzsymlink)
        for f in self.working_directory.listdir():
            if f.basename.startswith("."):
                continue
            if any(f.ext == skipext for skipext in (".pickle", ".r3d")):
                continue
            f.copy(self.results_directory)

        # Replace tmp working_directory with results_directory in coot scripts
        filenames = [
            self.results_directory.join(f) for f in ("coot.sh", "anom-coot.sh")
        ] + [
            py.path.local(f)
            for f in glob.glob(self.results_directory.join("*blob*-coot.py").strpath)
        ]
        for path in filenames:
            if path.check():
                logger.debug("Replacing tmp paths in %s", path)
                path.write(
                    path.read().replace(
                        self.working_directory.strpath, self.results_directory.strpath
                    )
                )
        if success:
            logger.info("Sending dimple results to ISPyB")
            success = self.send_results_to_ispyb()

        # Update SynchWeb tick hack file
        if self.params.get("synchweb_ticks") and self.params.get(
            "ispyb_parameters", {}
        ).get("set_synchweb_status"):
            if success:
                logger.debug("Removing SynchWeb hack file")
                py.path.local(self.params["synchweb_ticks"]).remove()
            else:
                logger.debug("Updating SynchWeb hack file to failure")
                py.path.local(self.params["synchweb_ticks"]).write(
                    "This file is used as a flag to synchweb to show the processing has failed"
                )

        return success
