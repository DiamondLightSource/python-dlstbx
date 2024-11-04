from __future__ import annotations

import pathlib
import shutil
import subprocess

from dlstbx.wrapper import Wrapper


class LigandFitWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.ligand_fit"

    def send_attachments_to_ispyb(self, pipeline_directory):
        for f in pipeline_directory.iterdir():
            if f.stem.endswith == "final":
                file_type = "Result"
                importance_rank = 1
            elif f.suffix == ".log":
                file_type = "Log"
                importance_rank = 2
            try:
                result_dict = {
                    "file_path": str(pipeline_directory),
                    "file_name": f.name,
                    "file_type": file_type,
                    "importance_rank": importance_rank,
                }
                self.record_result_individual_file(result_dict)
                self.log.info(f"Uploaded {f.name} as an attachment")
            except Exception:
                self.log.warning(f"Could not attach {f.name} to ISPyB", exc_info=True)

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.debug(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        # get params
        params = self.recwrap.recipe_step["job_parameters"]

        pdb = params.get("pdb")  # what if more than one pdb, see metal id
        if not pdb:
            self.log.error("Aborting ligand fit processing. PDB file not provided.")
            return False  # need better check of PDB file format?

        mtz = params.get("mtz")
        if not mtz:
            self.log.error("Aborting ligand fit processing. MTZ file not provided.")
            return False

        # ligand_code = params["ligand_code"]
        smiles = params.get("smiles")
        if not smiles:
            self.log.error(
                "Aborting ligand fit processing. Ligand SMILES string not provided."
            )
            return False

        pipeline = params.get("pipeline")
        pipelines = ["phenix", "phenix_pipeline"]  # add more options?
        if pipeline not in pipelines:
            self.log.error("Aborting ligand fit processing. Pipeline not recognised")
            return False

        working_directory = pathlib.Path(params["working_directory"])
        working_directory.mkdir(parents=True, exist_ok=True)
        results_directory = pathlib.Path(params["results_directory"])  # needed?
        results_directory.mkdir(parents=True, exist_ok=True)
        with open(working_directory / "LIG.smi", "w") as smi_file:
            smi_file.write(smiles)

        if pipeline == "phenix":
            phenix_command = f"phenix.ligandfit data={mtz}  model={pdb} ligand=LIG.smi"  # ligand={ligand_code}
        elif pipeline == "phenix_pipeline":
            phenix_command = f"phenix.ligand_pipeline {pdb} {mtz} LIG.smi"  # ligand_code={ligand_code}

        try:
            result = subprocess.run(
                phenix_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=working_directory,
            )

        except subprocess.CalledProcessError:
            self.log.error("Ligand_fit process failed")
            return False

        with open(working_directory / "ligand_fit.log", "w") as log_file:
            log_file.write(result.stdout)

        for f in working_directory.iterdir():
            if f.name.startswith("."):
                continue
            shutil.copytree(f, results_directory)

        pipeline_directory = (
            results_directory / "pipeline_1"
        )  # will work only when pipeline=phenix_pipeline

        self.log.info("Sending results to ISPyB")
        self.send_attachments_to_ispyb(pipeline_directory)

        self.log.info("Ligand_fitting pipeline finished")
        return True
