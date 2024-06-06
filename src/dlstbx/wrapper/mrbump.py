from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path

from iotbx.bioinformatics import fasta_sequence

import dlstbx.util.symlink
from dlstbx.util import iris
from dlstbx.wrapper import Wrapper
from dlstbx.wrapper.helpers import copy_results


class MrBUMPWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.mrbump"

    def construct_script(
        self, params: dict, working_directory: Path, hklin: str, seq_filename: str
    ):
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

    def setup(self, working_directory: Path, params: dict) -> bool:
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory, params["create_symlink"], levels=1
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
                            str(Path(data_directory.name) / Path(pth).name)
                        )
                    self.recwrap.environment.update({"hklin": hkl_paths})
                elif key == "localfile":
                    localfile_paths = []
                    for localfile in val:
                        shutil.copy(localfile, data_directory)
                        localfile_paths.append(
                            str(Path(data_directory.name) / Path(localfile).name)
                        )
                    self.recwrap.environment.update({"localfile": localfile_paths})
            try:
                tmp_path = working_directory / "TMP"
                tmp_path.mkdir(parents=True, exist_ok=True)
                pdblocal = params["mrbump"]["pdblocal"]
                iris.write_mrbump_singularity_script(
                    working_directory,
                    singularity_image,
                    tmp_path.name,
                    pdblocal,
                )
                self.recwrap.environment.update(
                    {"singularity_image": singularity_image}
                )
            except Exception:
                self.log.exception("Error writing singularity script")
                return False
        return True

    def run_mrbump(self, working_directory: Path, params: dict) -> bool:
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
        subprocess_directory = working_directory / params["create_symlink"]
        subprocess_directory.mkdir(parents=True, exist_ok=True)

        seq_filename = subprocess_directory / f"seq_{params['dcid']}.fasta"
        seq_filename.write_text(fasta_sequence(sequence).format(80))

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
            params, subprocess_directory, hklin, seq_filename
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
                        f"{k} {str(working_directory / vi)} CHAIN ALL"
                        for vi in self.recwrap.environment.get("localfile")
                    ]
                else:
                    localfile = [f"{k} {vi} CHAIN ALL" for vi in v]
            else:
                # Everything in ispyb_parameters is a list, but we're only interested
                # in the first item (there should only be one item)
                stdin_params[k] = v[0]
        stdin_list = localfile + [f"{k} {v}" for k, v in stdin_params.items()]
        stdin_str = "\n".join(stdin_list) + "\nEND"
        self.log.info("mrbump stdin: %s", stdin_str)
        success = True
        with (subprocess_directory / "MRBUMP.log").open("w") as fp:
            try:
                start_time = time.perf_counter()
                result = subprocess.run(
                    ["sh", mrbump_script],
                    cwd=subprocess_directory,
                    text=True,
                    input=stdin_str,
                    stdout=fp,
                    timeout=params.get("timeout"),
                )
                runtime = time.perf_counter() - start_time
                self.log.info(f"MrBUMP took {runtime:.1f} seconds")
            except subprocess.TimeoutExpired as te:
                success = False
                self.log.warning(f"MrBUMP timed out: {te.timeout}\n  {te.cmd}")
                self.log.debug(te.stdout)
                self.log.debug(te.stderr)
            else:
                if result.returncode:
                    self.log.info(f"MrBUMP failed with exitcode {result.returncode}")
                    self.log.debug(result.stdout)
                    self.log.debug(result.stderr)
                    success = False
                else:
                    hklout = subprocess_directory / Path(
                        params["mrbump"]["command"]["hklout"]
                    )
                    xyzout = subprocess_directory / Path(
                        params["mrbump"]["command"]["xyzout"]
                    )
                    success = hklout.is_file() and xyzout.is_file()
                    if success:
                        self.log.info("MrBUMP successful")
                    else:
                        self.log.info(
                            f"MrBUMP output files {hklout} or {xyzout} not found"
                        )
        if params.get("s3echo"):
            minio_client = iris.get_minio_client(params["s3echo"]["configuration"])
            bucket_name = params["s3echo"].get("bucket", "mrbump")
            try:
                iris.store_results_in_s3(
                    minio_client,
                    bucket_name,
                    params["rpid"],
                    subprocess_directory,
                    self.log,
                )
            except Exception:
                success = False
                self.log.info(
                    "Error while trying to save MrBUMP processing results to S3 Echo",
                    exc_info=True,
                )
        return success

    def run_report(self, working_directory: Path, params: dict, success: bool) -> bool:
        working_directory = working_directory / params.get("create_symlink", "")
        if not working_directory.is_dir():
            self.log.error(f"Output directory {working_directory} doesn't exist")
            return False

        if params.get("results_directory"):
            results_directory = Path(params["results_directory"]) / params.get(
                "create_symlink", ""
            )
            self.log.info(f"Copying MrBUMP results to {results_directory}")
            skip_copy = [".launch", ".recipewrap"]
            copy_results(
                str(working_directory),
                str(results_directory),
                skip_copy,
                self.log,
            )
            # Create symlink to results directory
            if params.get("create_symlink"):
                dlstbx.util.symlink.create_parent_symlink(
                    results_directory, params["create_symlink"]
                )

            hklout = Path(params["mrbump"]["command"]["hklout"])
            xyzout = Path(params["mrbump"]["command"]["xyzout"])
            success = hklout.is_file() and xyzout.is_file() and success

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

    def run(self) -> bool:
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = dict(self.recwrap.recipe_step["job_parameters"])

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

        if stage in {None, "report"}:
            success = self.run_report(working_directory, params, success)

        return success
