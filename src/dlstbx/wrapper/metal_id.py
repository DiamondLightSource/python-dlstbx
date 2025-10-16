from __future__ import annotations

import configparser
import json
import os
import pathlib
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import dlstbx.util.symlink
from dlstbx import schemas
from dlstbx.wrapper import Wrapper


@dataclass
class PeakData:
    density: float
    rmsd: float
    xyz: tuple[float, float, float]


class MetalIdWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.metal_id"

    def parse_peak_data(self, peak_data_file: pathlib.Path) -> list[PeakData]:
        peak_data: list[PeakData] = []
        with open(peak_data_file, "r") as file:
            for line in file:
                match = re.match(
                    r"Peak \d+: Electron Density = ([\d.]+) e/Å\^3, RMSD = ([\d.]+), XYZ = \((-?[\d.]+), (-?[\d.]+), (-?[\d.]+)\)",
                    line,
                )
                if match:
                    density = float(match.group(1))
                    rmsd = float(match.group(2))
                    xyz = (
                        float(match.group(3)),
                        float(match.group(4)),
                        float(match.group(5)),
                    )
                    peak_data.append(PeakData(density=density, rmsd=rmsd, xyz=xyz))
        return peak_data

    def send_results_to_ispyb(
        self,
        peak_data: list[PeakData],
        metal_id_command: str,
        dimple_log_file: pathlib.Path,
        results_directory: pathlib.Path,
        start_time: datetime,
        scaling_id: int,
    ) -> dict[str, Any]:
        self.log.info(
            f"Autoproc_prog_id: '{self.recwrap.environment.get('ispyb_autoprocprogram_id')}'"
        )

        dimple_log = configparser.RawConfigParser()
        dimple_log.read(dimple_log_file)

        end_time = datetime.now()
        mxmrrun = schemas.MXMRRun(
            auto_proc_scaling_id=scaling_id,
            auto_proc_program_id=self.recwrap.environment.get(
                "ispyb_autoprocprogram_id"
            ),
            rfree_start=dimple_log.getfloat("refmac5 restr", "ini_free_r"),
            rfree_end=dimple_log.getfloat("refmac5 restr", "free_r"),
            rwork_start=dimple_log.getfloat("refmac5 restr", "ini_overall_r"),
            rwork_end=dimple_log.getfloat("refmac5 restr", "overall_r"),
        )

        blobs = []
        for n_peak, peak in enumerate(peak_data, start=1):
            self.log.info(
                f"Adding blob {n_peak} to ispyb results - Density: {peak.density}, rmsd: {peak.rmsd}, xyz: {peak.xyz}"
            )
            blobs.append(
                schemas.Blob(
                    xyz=peak.xyz,
                    height=peak.density,
                    # nearest_atom=nearest_atom,
                    # nearest_atom_distance=distance,
                    map_type="difference",  # TODO change this to anomalous_difference once enum exists.
                    filepath=results_directory,
                    view1=f"peak_{n_peak}.png",
                )
            )

        app = schemas.AutoProcProgram(
            command_line=metal_id_command,
            programs="metal_id",
            status=1,
            message="processing successful",
            start_time=start_time,
            end_time=end_time,
        )

        attachments = []

        primary_result_files = self.params.get("primary_result_files", {})
        self.log.info("Adding attachments for upload to ispyb")
        for f in results_directory.iterdir():
            if f.name in primary_result_files:
                file_type = primary_result_files[f.name]["type"]
                importance_rank = primary_result_files[f.name]["rank"]
            elif f.suffix in [".map", ".pdb", ".dat", ".pha", ".mtz"]:
                file_type = "result"
                importance_rank = 2
            else:
                continue

            attachments.append(
                schemas.Attachment(
                    file_type=file_type,
                    file_path=f.parent,
                    file_name=f.name,
                    timestamp=end_time,
                    importance_rank=importance_rank,
                )
            )
            self.log.info(f"Added {f.name} as an attachment")

        if getattr(self, "final_directory", None):
            for att in attachments:
                if att.file_name in primary_result_files:
                    shutil.copy(att.file_path / att.file_name, self.final_directory)
                    att.file_path = self.final_directory
            for blob in blobs:
                if blob.filepath and blob.view1:
                    shutil.copy(blob.filepath / blob.view1, self.final_directory)
                    blob.filepath = self.final_directory

        ispyb_results = {
            "mxmrrun": json.loads(mxmrrun.model_dump_json()),
            "blobs": [json.loads(blob.model_dump_json()) for blob in blobs],
            "auto_proc_program": json.loads(app.model_dump_json()),
            "attachments": [
                json.loads(attachment.model_dump_json()) for attachment in attachments
            ],
        }

        return ispyb_results

    def run(self):
        start_time = datetime.now()
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.debug(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )
        # Get parameters from the recipe file
        self.params = self.recwrap.recipe_step["job_parameters"]

        scaling_id = self.params.get("scaling_id", [])
        if len(scaling_id) != 1:
            self.log.info(f"Scaling ID {scaling_id} provided")
            self.log.error(
                "Exactly one scaling_id must be provided - cannot run metal_id"
            )
            return False
        scaling_id = scaling_id[0]

        src_mtz_files = self.params.get("data", [])
        if not src_mtz_files:
            self.log.error("Could not identify on what data to run")
            return False
        if len(src_mtz_files) != 2:
            self.log.error(
                f"Exactly two data files need to be provided, {len(src_mtz_files)} files were given"
            )
            return False

        working_directory = pathlib.Path(self.params["working_directory"])
        working_directory.mkdir(parents=True, exist_ok=True)

        results_directory = pathlib.Path(self.params["results_directory"])
        results_directory.mkdir(parents=True, exist_ok=True)

        # Copy the source mtz_files files to the working directory
        mtz_files = []
        for _file in src_mtz_files:
            _file_name = os.path.basename(_file)
            _dest_file = working_directory / _file_name
            # If input mtz files have the same file name (e.g. fast_dp.mtz), add number to differentiate files
            if _dest_file in mtz_files:
                _dest_file = _dest_file.with_name(
                    f"{_dest_file.stem}_{len(mtz_files)}{_dest_file.suffix}"
                )
            try:
                shutil.copy(_file, _dest_file)
                self.log.info(f"File '{_file}' copied to '{_dest_file}'")
                mtz_files.append(_dest_file)
            except FileNotFoundError:
                self.log.error(f"Source file '{_file}' not found.")
                return False
            except PermissionError:
                self.log.error(
                    f"Permission denied for copying '{_file}' to '{_dest_file}'."
                )
                return False

        mtz_below = working_directory / mtz_files[0]
        mtz_above = working_directory / mtz_files[1]

        pdb_files_and_codes = self.params["pdb"]

        pdb_files = []
        for file_or_code in pdb_files_and_codes:
            if not os.path.isfile(file_or_code) and len(file_or_code) == 4:
                local_pdb_copy = pathlib.Path(
                    f"/dls/science/groups/scisoft/PDB/{file_or_code[1:3].lower()}/pdb{file_or_code.lower()}.ent.gz"
                )
                if local_pdb_copy.is_file():
                    file_or_code = local_pdb_copy
                    self.log.debug(f"Using local PDB {local_pdb_copy}")
            if os.path.isfile(file_or_code):
                shutil.copy(file_or_code, working_directory)
                pdb_files.append(
                    str(working_directory / os.path.basename(file_or_code))
                )

        output_directory = working_directory / "metal_id"

        # Run metal_id
        metal_id_command = f"metal_id {mtz_above} {mtz_below} {' '.join(pdb_files)} -o {output_directory}"
        self.log.debug(f"Running metal_id command: '{metal_id_command}'")

        subprocess.run(
            metal_id_command,
            shell=True,
            cwd=working_directory,
        )

        self.log.debug("Reading in peak data")
        peak_file = output_directory / "found_peaks.dat"
        if not peak_file.is_file():
            self.log.info("Metal_ID: No peaks found")
            peak_data = []
        else:
            peak_data = self.parse_peak_data(peak_file)

        for f in output_directory.iterdir():
            self.log.debug(f"Searching for files to copy. Current file is : {f}")
            if f.is_dir():
                continue
            if f.name.startswith("."):
                continue
            self.log.debug("Copying file")
            shutil.copy(f, results_directory)

        symlink = self.params.get("create_symlink")
        if isinstance(symlink, list):
            symlink = symlink[0]
        if symlink:
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(output_directory), symlink
            )
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(results_directory), symlink
            )

        self.log.info("Sending results to ISPyB")

        dimple_log = working_directory / "metal_id" / "dimple_below" / "dimple.log"
        if not dimple_log.is_file():
            self.log.error(
                f"dimple log file '{dimple_log}' not found - cannot insert metal_id results to ISPyB"
            )
            return False

        if pipeine_final_params := self.params.get("pipeline-final", []):
            self.final_directory = pathlib.Path(pipeine_final_params["path"])
            self.final_directory.mkdir(parents=True, exist_ok=True)
            if self.params.get("create_symlink"):
                dlstbx.util.symlink.create_parent_symlink(self.final_directory, symlink)

        ispyb_results = self.send_results_to_ispyb(
            peak_data,
            metal_id_command,
            dimple_log,
            results_directory,
            start_time,
            scaling_id,
        )

        self.log.info(f"Sending {str(ispyb_results)} to ispyb service")
        self.recwrap.send_to("ispyb", ispyb_results)

        self.log.info("Metal_ID script finished")
        return True
