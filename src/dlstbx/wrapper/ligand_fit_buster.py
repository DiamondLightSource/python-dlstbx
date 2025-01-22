from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
from shutil import ignore_patterns

import dlstbx.util.symlink
from dlstbx.wrapper import Wrapper


class LigandFitBusterWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.ligand_fit_buster"

    def pull_CC_from_log(self, results_directory):
        f = results_directory / "rhofit" / "Hit_corr.log"
        file_read = open(f, "r")
        lines = file_read.readlines()
        Hit_ccs = []
        for line in lines:
            Hit_ccs.append(float((line.split(" ")[1])))
        file_read.close()
        CC = max(Hit_ccs)
        return CC

    def send_attachments_to_ispyb(self, results_directory, min_cc_keep):
        CC = self.pull_CC_from_log(results_directory)
        final_results = [
            "BUSTER_model.pdb",
            "BUSTER_refln.mtz",
            "report.pdf",
            "buster-refine.log",
        ]
        for f in results_directory.rglob("*"):
            if f.name in final_results and CC >= min_cc_keep and f.suffix != ".log":
                file_type = "Result"
                importance_rank = 1
            elif f.name in final_results and f.suffix == ".log":
                file_type = "Log"
                importance_rank = 2
            else:
                continue
            try:
                result_dict = {
                    "file_path": str(f.parents[0]),
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

        params = self.recwrap.recipe_step["job_parameters"]

        pdb = pathlib.Path(params.get("pdb"))
        if not pdb.is_file():
            self.log.error(
                f"Aborting ligand fit processing. PDB file {pdb} does not exist."
            )
            return False

        mtz = pathlib.Path(params.get("mtz"))
        if not mtz.is_file():
            self.log.error(
                f"Aborting ligand fit processing. MTZ file {mtz} does not exist."
            )
            return False

        smiles = params.get("smiles")
        min_cc_keep = params.get("min_cc_keep")

        working_directory = pathlib.Path(params["working_directory"])
        working_directory.mkdir(parents=True, exist_ok=True)
        results_directory = pathlib.Path(params["results_directory"])
        results_directory.mkdir(parents=True, exist_ok=True)

        with open(working_directory / "LIG.smi", "w") as smi_file:
            smi_file.write(smiles)

        buster_commands = [
            "grade -resname LIG -in LIG.smi",
            f"rhofit -l grade-LIG.cif -m {mtz} -p {pdb}",
            "hydrogenate -p rhofit/merged.pdb -o merged_hydrogenated.pdb -ligonly -zero -l grade-LIG.cif",
            f"refine -p merged_hydrogenated.pdb -m {mtz} -d buster-refine -l grade-LIG.cif -report -M TLSbasic UseCcp4MonomerLibrary=yes StopOnGellySanityCheckError=no > buster-refine.log",
        ]

        for command in buster_commands:
            try:
                result = subprocess.run(
                    command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    cwd=working_directory,
                    check=True,
                    timeout=params.get("timeout-minutes") * 60,
                )

            except subprocess.CalledProcessError as e:
                self.log.error(f"Ligand_fit process '{command}' failed")
                self.log.info(e.stdout)
                self.log.error(e.stderr)
                return False

        with open(working_directory / "ligand_fit_buster.log", "w") as log_file:
            log_file.write(result.stdout)

        shutil.copytree(
            working_directory,
            results_directory,
            dirs_exist_ok=True,
            ignore=ignore_patterns(".*"),
            symlinks=False,
            ignore_dangling_symlinks=True,
        )

        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(working_directory), params["create_symlink"]
            )
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(results_directory), params["create_symlink"]
            )

        self.log.info("Sending results to ISPyB")
        self.send_attachments_to_ispyb(results_directory, min_cc_keep)

        CC = self.pull_CC_from_log(results_directory)
        if CC >= min_cc_keep:
            self.log.info("Ligand_fitting pipeline finished successfully")
            return True
        else:
            self.log.info(
                f"Ligand_fitting pipeline finished but ligand fitting CC ({CC}) did not meet quality threshold ({min_cc_keep})"
            )
            return False
