from __future__ import annotations

import pathlib
import subprocess

from dlstbx.wrapper import Wrapper


class LigandFitWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.ligand_fit"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.debug(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        # get params
        params = self.recwrap.recipe_step["job_parameters"]

        pdb = params.get("pdb")  # needs handling if not provided
        if not pdb:
            self.log.error(
                "Aborting ligand fit processing. PDB file not provided."
            )  # do again for other params
            return False  # need better check of PDB file format, also mtz

        mtz = params.get("mtz")
        # ligand_code = params["ligand_code"]
        smiles = params.get("smiles")
        pipeline = params.get("pipeline")

        pipelines = ["phenix", "phenix_pipeline"]
        if pipeline not in pipelines:
            self.log.error("Aborting ligand fit processing. Pipeline not recognised")
            return False

        working_directory = pathlib.Path(params["working_directory"])
        working_directory.mkdir(parents=True, exist_ok=True)
        with open(working_directory / "LIG.smi", "w") as smi_file:
            smi_file.write(smiles)

        if pipeline == "phenix":
            phenix_command = f"phenix.ligandfit data={mtz}  model={pdb} ligand=LIG.smi"  # ligand={ligand_code}
            result = subprocess.run(
                phenix_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=working_directory,
            )

        elif pipeline == "phenix_pipeline":
            phenix_command = f"phenix.ligand_pipeline {pdb} {mtz} LIG.smi"  # ligand_code={ligand_code}
            result = subprocess.run(
                phenix_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=working_directory,
            )

        with open(working_directory / "ligand_fit.log", "w") as log_file:
            log_file.write(result.stdout)

        return True
