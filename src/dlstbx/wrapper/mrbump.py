import logging
import os
import pathlib
import shutil
import tempfile

import procrunner
import zocalo.wrapper
from iotbx.bioinformatics import fasta_sequence

import dlstbx.util.symlink

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
        with open(seq_filename, "w") as fp:
            fp.write(fasta_sequence(sequence).format(80))

        mrbump_script = ["#!/bin/bash", ". /etc/profile.d/modules.sh"]

        for mdl in module_params:
            mrbump_script.append(f"module load {mdl}")

        mrbump_command = ["mrbump"]

        hklin = params.get("ispyb_parameters", {}).get("hklin")
        if hklin:
            assert len(hklin) == 1, f"More than one hklin provided: {hklin}"
            mrbump_command.append(f"hklin {hklin[0]}")
        # Alternatively hklin could be provided in cdl_params

        for arg, val in cdl_params.items():
            mrbump_command.append(f"{arg} {val}")
        mrbump_command.append(f"seqin {seq_filename}")
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
        except OSError:
            logger.exception(
                "Could not create MrBUMP script file in the working directory"
            )
            return False

        return command_line, mrbump_filename

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = pathlib.Path(params["working_directory"])
        results_directory = pathlib.Path(params["results_directory"])

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

        working_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                str(working_directory), params["create_symlink"]
            )

        # Create results directory and symlink if they don't already exist
        results_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                str(results_directory), params["create_symlink"]
            )

        command, mrbump_script = self.construct_script(
            params, working_directory, sequence
        )
        logger.info("command: %s", command)
        stdin_params = params["mrbump"]["stdin"]

        # Extend stdin with those provided in ispyb_parameters
        localfiles = []
        for k, v in params.get("ispyb_parameters", {}).items():
            if k == "hklin":
                # This is provided as command line keyword and handled elsewhere
                continue
            if k == "localfiles":
                localfiles = [f"{k} {vi}" for vi in v]
            else:
                # Everything in ispyb_parameters is a list, but we're only interested
                # in the first item (there should only be one item)
                stdin_params[k] = v[0]

        stdin = localfiles + [f"{k} {v}" for k, v in stdin_params.items()]
        stdin = "\n".join(stdin) + "\nEND"
        logger.info("mrbump stdin: %s", stdin)

        with (working_directory / "MRBUMP.log").open("w") as fp:
            result = procrunner.run(
                ["sh", mrbump_script],
                stdin=stdin.encode("utf-8"),
                callback_stdout=lambda x: print(x, file=fp),
                working_directory=working_directory,
                timeout=params.get("timeout"),
            )
            success = not result["exitcode"] and not result["timeout"]
            hklout = pathlib.Path(params["mrbump"]["command"]["hklout"])
            xyzout = pathlib.Path(params["mrbump"]["command"]["xyzout"])
            success = success and hklout.is_file() and xyzout.is_file()
            if success:
                fp.write("Looks like MrBUMP succeeded")
                logger.info("mrbump successful, took %.1f seconds", result["runtime"])
            else:
                fp.write("Looks like MrBUMP failed")
                logger.info(
                    "mrbump failed with exitcode %s and timeout %s",
                    result["exitcode"],
                    result["timeout"],
                )
                logger.debug(result["stdout"].decode("latin1"))
                logger.debug(result["stderr"].decode("latin1"))

        logger.info(f"Copying MrBUMP results to {results_directory}")
        keep_ext = {".log": "log", ".mtz": "result", ".pdb": "result"}
        allfiles = []
        for filename in working_directory.iterdir():
            filetype = keep_ext.get(filename.suffix)
            if filetype is None:
                continue
            destination = results_directory / filename.name
            shutil.copy(filename, destination)
            allfiles.append(destination)
            if filetype:
                self.record_result_individual_file(
                    {
                        "file_path": str(destination.parent),
                        "file_name": destination.name,
                        "file_type": filetype,
                        "importance_rank": 1,
                    }
                )

        return success
