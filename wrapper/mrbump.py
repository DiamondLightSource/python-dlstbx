from __future__ import absolute_import, division, print_function

import logging
import py

import procrunner
import zocalo.wrapper
import os
import dlstbx.util.symlink
import tempfile

logger = logging.getLogger("dlstbx.wrap.mrbump")


class MrBUMPWrapper(zocalo.wrapper.BaseWrapper):
    def construct_script(self, params, working_directory, sequence):
        """Construct MrBUMP script line.
       Takes job parameter dictionary, returns array."""
        module_params = params["mrbump"]["modules"]
        cdl_params = params["mrbump"]["command"]

        seq_filename = os.path.join(
            working_directory, "seq_{}.fasta".format(params["dcid"])
        )
        from iotbx.bioinformatics import fasta_sequence

        with open(seq_filename, "w") as fp:
            fp.write(fasta_sequence(sequence).format(80))

        mrbump_script = ["#!/bin/bash", ". /etc/profile.d/modules.sh"]

        for mdl in module_params:
            mrbump_script.append("module load {}".format(mdl))

        mrbump_command = [
            "mrbump",
            "hklin {}".format(params["ispyb_parameters"]["hklin"]),
        ]
        for arg, val in cdl_params.items():
            mrbump_command.append("{} {}".format(arg, val))
        mrbump_command.append("seqin {}".format(seq_filename))
        command_line = " ".join(mrbump_command)

        mrbump_script.append(command_line)

        try:
            fp = tempfile.NamedTemporaryFile(dir=working_directory)
            mrbump_filename = os.path.join(
                working_directory, "run_mrbump_{}.sh".format(os.path.basename(fp.name))
            )
            fp.close()
            with open(mrbump_filename, "w") as fp:
                fp.write("\n".join(mrbump_script))
        except IOError:
            logger.exception(
                "Could not create MrBUMP script file in the working directory"
            )
            return False

        return command_line, mrbump_filename

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])

        try:
            sequence = params["protein_info"]["sequence"]
            if not sequence:
                logger.error("Aborting MrBUMP processing. Sequence data not available.")
                return False
        except Exception:
            logger.exception(
                "MrBUMP processing failed: Cannot read sequence information"
            )
            return False

        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory.strpath, params["create_symlink"]
            )

        command, mrbump_script = self.construct_script(
            params, working_directory.strpath, sequence
        )
        logger.info("command: %s", command)
        stdin_params = params["mrbump"]["stdin"]
        stdin = (
            "\n".join(["{} {}".format(k, v) for k, v in stdin_params.items()]) + "\nEND"
        )
        logger.info("mrbump stdin: %s", stdin)

        with open(os.path.join(working_directory.strpath, "MRBUMP.log"), "w") as fp:
            result = procrunner.run(
                ["sh", mrbump_script],
                stdin=stdin,
                callback_stdout=lambda x: print(x, file=fp),
                working_directory=working_directory.strpath,
                timeout=params.get("timeout"),
            )
            logger.info("command: %s", " ".join(result["command"]))
            logger.info("time_start: %s", result["time_start"])
            logger.info("time_end: %s", result["time_end"])
            logger.info("runtime: %s", result["runtime"])
            if result["exitcode"] or result["timeout"]:
                logger.info("timeout: %s", result["timeout"])
                logger.info("exitcode: %s", result["exitcode"])
                logger.debug(result["stdout"])
                logger.debug(result["stderr"])

            hklout = py.path.local(params["mrbump"]["command"]["hklout"])
            xyzout = py.path.local(params["mrbump"]["command"]["xyzout"])
            if hklout.check() and xyzout.check():
                fp.write("Looks like MrBUMP succeeded")
            else:
                fp.write("Looks like MrBUMP failed")
                return False

        # Create results directory and symlink if they don't already exist
        try:
            results_directory.ensure(dir=True)
            if params.get("create_symlink"):
                dlstbx.util.symlink.create_parent_symlink(
                    results_directory.strpath, params["create_symlink"]
                )

            logger.info("Copying MrBUMP results to %s", results_directory.strpath)
            keep_ext = {".log": "log", ".mtz": "result", ".pdb": "result"}
            allfiles = []
            for filename in working_directory.listdir():
                filetype = keep_ext.get(filename.ext)
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
        except NameError:
            logger.info(
                "Copying mrbump results ignored. Results directory unavailable."
            )

        return result["exitcode"] == 0
