from __future__ import annotations

import logging
import os
import pathlib
from pathlib import Path

import procrunner
from iotbx.bioinformatics import fasta_sequence

from dlstbx.wrapper import Wrapper

logger = logging.getLogger("dlstbx.wrap.mrbump_run")


class MrBUMPRunWrapper(Wrapper):
    def construct_script(self, params, working_directory, hklin, seq_filename):
        """Construct MrBUMP script line.
        Takes job parameter dictionary, returns array."""
        module_params = params["mrbump"].get("modules")
        cdl_params = params["mrbump"]["command"]

        mrbump_script = []

        if module_params:
            mrbump_script.extend(["#!/bin/bash", ". /etc/profile.d/modules.sh"])
            for mdl in module_params:
                mrbump_script.append(f"module load {mdl}")

        mrbump_command = ["mrbump"]

        mrbump_command.append(f"hklin {hklin}")

        for arg, val in cdl_params.items():
            mrbump_command.append(f"{arg} {val}")
        mrbump_command.append(f"seqin {seq_filename}")
        command_line = " ".join(mrbump_command)

        mrbump_script.append(command_line)

        try:
            mrbump_filename = os.path.join(working_directory, "run_mrbump.sh")
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

        working_directory = Path(params.get("working_directory", os.getcwd()))
        working_directory.mkdir(parents=True, exist_ok=True)

        try:
            sequence = params["protein_info"]["sequence"]
            if not sequence:
                logger.error("Aborting MrBUMP processing. Sequence data not available.")
                return False
        except Exception:
            logger.exception(
                "MrBUMP processing failed: Cannot read sequence information."
            )
            return False

        procrunner_directory = working_directory / params["create_symlink"]
        procrunner_directory.mkdir(parents=True, exist_ok=True)

        seq_filename = os.path.join(
            procrunner_directory, "seq_{}.fasta".format(params["dcid"])
        )
        with open(seq_filename, "w") as fp:
            fp.write(fasta_sequence(sequence).format(80))

        if self.recwrap.environment.get("singularity_image"):
            hklin = self.recwrap.environment.get("hklin")
            # Alternatively hklin could be provided in cdl_params
            if hklin:
                assert len(hklin) == 1, f"More than one hklin provided: {hklin}"
                hklin = working_directory / hklin[0]
            else:
                logger.error(
                    "MrBUMP processing failed: Input hklin file not specified."
                )
                return False
        else:
            hklin = params.get("ispyb_parameters", {}).get("hklin")
            # Alternatively hklin could be provided in cdl_params
            if hklin:
                assert len(hklin) == 1, f"More than one hklin provided: {hklin}"
                hklin = hklin[0]
            else:
                logger.error(
                    "MrBUMP processing failed: Input hklin file not specified."
                )
                return False

        command, mrbump_script = self.construct_script(
            params, procrunner_directory, hklin, seq_filename
        )
        logger.info("command: %s", command)
        stdin_params = params["mrbump"]["stdin"]

        # Extend stdin with those provided in ispyb_parameters
        localfile = []
        for k, v in params.get("ispyb_parameters", {}).items():
            if k == "hklin":
                # This is provided as command line keyword and handled elsewhere
                continue
            if k == "localfile":
                if self.recwrap.environment.get("singularity_image"):
                    localfile = [
                        f"{k} {str(working_directory / vi)}"
                        for vi in self.recwrap.environment.get("localfile")
                    ]
                else:
                    localfile = [f"{k} {vi}" for vi in v]
            else:
                # Everything in ispyb_parameters is a list, but we're only interested
                # in the first item (there should only be one item)
                stdin_params[k] = v[0]

        stdin = localfile + [f"{k} {v}" for k, v in stdin_params.items()]
        stdin = "\n".join(stdin) + "\nEND"
        logger.info("mrbump stdin: %s", stdin)

        with (procrunner_directory / "MRBUMP.log").open("w") as fp:
            result = procrunner.run(
                ["sh", mrbump_script],
                stdin=stdin.encode("utf-8"),
                callback_stdout=lambda x: print(x, file=fp),
                working_directory=procrunner_directory,
                timeout=params.get("timeout"),
            )
        success = not result["exitcode"] and not result["timeout"]
        hklout = procrunner_directory / pathlib.Path(
            params["mrbump"]["command"]["hklout"]
        )
        xyzout = procrunner_directory / pathlib.Path(
            params["mrbump"]["command"]["xyzout"]
        )
        success = success and hklout.is_file() and xyzout.is_file()
        if success:
            logger.info("mrbump successful, took %.1f seconds", result["runtime"])
        else:
            logger.info(
                "mrbump failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            logger.debug(result["stdout"].decode("latin1"))
            logger.debug(result["stderr"].decode("latin1"))

        return success
