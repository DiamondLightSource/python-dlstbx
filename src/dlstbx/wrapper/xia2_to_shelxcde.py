from __future__ import annotations

import glob
import os
import pathlib
import re
import shutil
import subprocess
import time

import dlstbx.util.symlink
from dlstbx.util.shelxc import parse_shelxc_logs
from dlstbx.wrapper import Wrapper


class Xia2toShelxcdeWrapper(Wrapper):
    _logger_name = "zocalo.wrap.xia2.to_shelxcde"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = pathlib.Path(params.get("working_directory", os.getcwd()))
        working_directory.mkdir(parents=True, exist_ok=True)

        # Create working directory with symbolic link
        if params.get("create_symlink"):
            try:
                levels = params["levels_symlink"]
                dlstbx.util.symlink.create_parent_symlink(
                    working_directory, params["create_symlink"], levels=levels
                )
            except KeyError:
                dlstbx.util.symlink.create_parent_symlink(
                    working_directory, params["create_symlink"]
                )

        data_files = sorted(
            pth
            for pth in glob.glob(
                str(pathlib.Path(params["data"]).parent / "**"), recursive=True
            )
            if re.search(params["data"], pth)
        )
        if not data_files:
            self.log.error(
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
        self.log.info("Generating SHELXC .ins file")
        self.log.info("command: %s", " ".join(command))
        try:
            start_time = time.perf_counter()
            result = subprocess.run(
                command,
                cwd=working_directory,
                text=True,
                timeout=params.get("timeout"),
            )
            runtime = time.perf_counter() - start_time
            self.log.info(f"runtime: {runtime: .1f} seconds")
        except subprocess.TimeoutExpired as te:
            self.log.info(f"timeout: {te.timeout}")
            self.log.debug(te.stdout)
            self.log.debug(te.stderr)
        else:
            if result.returncode:
                self.log.info(f"exitcode: {result.returncode}")
                self.log.debug(result.stdout)
                self.log.debug(result.stderr)
        command = ["sh", "shelxc.sh"]
        self.log.info("Starting SHELXC")
        self.log.info("command: %s", " ".join(command))
        try:
            start_time = time.perf_counter()
            result = subprocess.run(
                command,
                cwd=working_directory,
                capture_output=True,
                text=True,
                timeout=params.get("timeout"),
            )
            runtime = time.perf_counter() - start_time
            self.log.info(f"runtime: {runtime: .1f} seconds")
        except subprocess.TimeoutExpired as te:
            self.log.info(f"timeout: {te.timeout}")
            self.log.debug(te.stdout)
            self.log.debug(te.stderr)
        else:
            if result.returncode:
                self.log.info(f"exitcode: {result.returncode}")
                self.log.debug(result.stdout)
                self.log.debug(result.stderr)
        if not result.stdout:
            self.log.debug("SHELXC log is empty")
            return False

        with (working_directory / "results_shelxc.log").open("w") as fp:
            fp.write(result.stdout)
        stats = parse_shelxc_logs(result.stdout, self.log)
        if not stats:
            self.log.debug("Cannot process SHELXC data. Aborting.")
            return False
        self.recwrap.send_to("downstream", stats)

        # Create results directory and symlink if they don't already exist
        try:
            results_directory = pathlib.Path(params["results_directory"])
            results_directory.mkdir(parents=True, exist_ok=True)
        except KeyError:
            self.log.debug("Result directory not specified")
        try:
            self.log.info("Copying SHELXC results to %s", results_directory)
            if params.get("create_symlink"):
                try:
                    levels = params["levels_symlink"]
                    dlstbx.util.symlink.create_parent_symlink(
                        results_directory,
                        params["create_symlink"],
                        levels=levels,
                    )
                except KeyError:
                    dlstbx.util.symlink.create_parent_symlink(
                        results_directory, params["create_symlink"]
                    )
            for f in working_directory.iterdir():
                if f.suffix in [".log", ".hkl", ".sh", ".ins", ".cif"]:
                    shutil.copy(f, results_directory)
        except NameError:
            self.log.debug(
                "Ignore copying SHELXC results. Results directory not specified."
            )
        return result.returncode == 0
