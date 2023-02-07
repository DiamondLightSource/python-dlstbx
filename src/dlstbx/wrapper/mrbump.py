from __future__ import annotations

import os
import shutil
from pathlib import Path

import procrunner
from iotbx.bioinformatics import fasta_sequence

import dlstbx.util.symlink
from dlstbx.util.big_ep_helpers import copy_results
from dlstbx.util.iris import write_mrbump_singularity_script
from dlstbx.wrapper import Wrapper


class MrBUMPWrapper(Wrapper):

    _logger_name = "dlstbx.wrap.mrbump"

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
            self.log.exception(
                "Could not create MrBUMP script file in the working directory"
            )
            return False

        return command_line, mrbump_filename

    def setup(self, working_directory, params):
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                str(working_directory), params["create_symlink"], levels=1
            )
        singularity_image = params.get("singularity_image")
        if singularity_image:
            # Copy files into mrbump_data directory for HTCondor transfer
            data_directory = working_directory / "mrbump_data"
            data_directory.mkdir(parents=True, exist_ok=True)
            # Extend stdin with those provided in ispyb_parameters
            for key, val in params.get("ispyb_parameters", {}).items():
                if key == "hklin":
                    # This is provided as command line keyword and handled elsewhere
                    hkl_paths = []
                    for pth in val:
                        shutil.copy(pth, data_directory)
                        hkl_paths.append(
                            os.sep.join([data_directory.name, os.path.basename(pth)])
                        )
                    self.recwrap.environment.update({"hklin": hkl_paths})
                elif key == "localfile":
                    localfile_paths = []
                    for localfile in val:
                        shutil.copy(localfile, data_directory)
                        localfile_paths.append(
                            os.sep.join(
                                [data_directory.name, os.path.basename(localfile)]
                            )
                        )
                    self.recwrap.environment.update({"localfile": localfile_paths})
            try:
                tmp_path = working_directory / "TMP"
                tmp_path.mkdir(parents=True, exist_ok=True)
                pdblocal = Path(params["mrbump"]["pdblocal"])
                # shutil.copy(singularity_image, str(working_directory))
                # image_name = Path(singularity_image).name
                write_mrbump_singularity_script(
                    working_directory,
                    singularity_image,
                    tmp_path.name,
                    str(pdblocal),
                )
                self.recwrap.environment.update(
                    {"singularity_image": singularity_image}
                )
            except Exception:
                self.log.exception("Error writing singularity script")
                return False
        return True

    def run_mrbump(self, working_directory, params):
        try:
            sequence = params["protein_info"]["sequence"]
            if not sequence:
                self.log.error(
                    "Aborting MrBUMP processing. Sequence data not available."
                )
                return False
        except Exception:
            self.log.exception(
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
                self.log.error(
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
                self.log.error(
                    "MrBUMP processing failed: Input hklin file not specified."
                )
                return False
        command, mrbump_script = self.construct_script(
            params, procrunner_directory, hklin, seq_filename
        )
        self.log.info("command: %s", command)
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
        self.log.info("mrbump stdin: %s", stdin)
        with (procrunner_directory / "MRBUMP.log").open("w") as fp:
            result = procrunner.run(
                ["sh", mrbump_script],
                stdin=stdin.encode("utf-8"),
                callback_stdout=lambda x: print(x, file=fp),
                working_directory=procrunner_directory,
                timeout=params.get("timeout"),
            )
        success = not result["exitcode"] and not result["timeout"]
        hklout = procrunner_directory / Path(params["mrbump"]["command"]["hklout"])
        xyzout = procrunner_directory / Path(params["mrbump"]["command"]["xyzout"])
        success = success and hklout.is_file() and xyzout.is_file()
        if success:
            self.log.info("mrbump successful, took %.1f seconds", result["runtime"])
        else:
            self.log.info(
                "mrbump failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            self.log.debug(result["stdout"].decode("latin1"))
            self.log.debug(result["stderr"].decode("latin1"))
        return success

    def run_report(self, working_directory, params):
        if params.get("results_directory"):
            results_directory = Path(params["results_directory"])
            self.log.info(f"Copying MrBUMP results to {results_directory}")
            skip_copy = [".launch", ".recipewrap"]
            copy_results(
                str(working_directory / params["create_symlink"]),
                str(results_directory),
                skip_copy,
                self.log,
            )
        # Create results directory and symlink if they don't already exist
        results_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                str(results_directory), params["create_symlink"]
            )
        hklout = Path(params["mrbump"]["command"]["hklout"])
        xyzout = Path(params["mrbump"]["command"]["xyzout"])
        success = hklout.is_file() and xyzout.is_file()
        keep_ext = {".log": "log", ".mtz": "result", ".pdb": "result"}
        for filename in results_directory.iterdir():
            filetype = keep_ext.get(filename.suffix)
            if filetype is None:
                continue
            if filetype:
                self.record_result_individual_file(
                    {
                        "file_path": str(filename.parent),
                        "file_name": filename.name,
                        "file_type": filetype,
                        "importance_rank": 1,
                    }
                )
        return success

    def run(self):

        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]

        # Create working directory with symbolic link
        working_directory = Path(params.get("working_directory", os.getcwd()))
        working_directory.mkdir(parents=True, exist_ok=True)

        stage = params.get("stage")
        assert stage in {None, "setup", "run", "report"}
        success = True

        if stage in {None, "setup"}:
            success = self.setup(working_directory, params)

        if stage in {None, "run"} and success:
            success = self.run_mrbump(working_directory, params)

        if stage in {None, "report"} and success:
            success = self.run_report(working_directory, params)

        return success
