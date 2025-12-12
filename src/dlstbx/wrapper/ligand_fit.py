from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import subprocess
from shutil import ignore_patterns

# import molviewspec as mvs
import dlstbx.util.symlink
from dlstbx.wrapper import Wrapper


class LigandFitWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.ligand_fit"

    def pull_CC_from_log(self, pipeline_directory):
        f = pipeline_directory / "pipeline.log"
        file_read = open(f, "r")
        text = "CC"
        lines = file_read.readlines()
        llist = []
        for line in lines:
            if text in line:
                match = re.search(r"CC\s*=\s*([0-9.]+)", line)
                if match:
                    llist.append(match.group(1))
        file_read.close()
        CC = llist[-1]  # take the final CC
        return CC

    def send_attachments_to_ispyb(self, pipeline_directory, final_directory):
        for f in pipeline_directory.iterdir():
            if f.stem.endswith("final"):
                file_type = "Result"
                importance_rank = 1
            elif f.suffix == ".html":
                file_type = "Result"
                importance_rank = 1
            elif f.suffix == ".png":
                file_type = "Result"
                importance_rank = 1
            elif f.suffix == ".json":
                file_type = "Result"
                importance_rank = 1
            elif f.suffix == ".log":
                file_type = "Log"
                importance_rank = 2
            else:
                continue
            try:
                shutil.copy(pipeline_directory / f.name, final_directory)
                result_dict = {
                    "file_path": str(final_directory),
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

        pipeline = params.get("pipeline")
        pipelines = ["phenix_pipeline"]
        if pipeline not in pipelines:
            self.log.error(
                f"Aborting ligand fit processing. Pipeline '{pipeline}' not recognised"
            )
            return False

        working_directory = pathlib.Path(params["working_directory"])
        working_directory.mkdir(parents=True, exist_ok=True)
        results_directory = pathlib.Path(params["results_directory"])
        results_directory.mkdir(parents=True, exist_ok=True)

        if pipeline_final_params := params.get("pipeline-final", []):
            final_directory = pathlib.Path(pipeline_final_params["path"])
            final_directory.mkdir(parents=True, exist_ok=True)
            if params.get("create_symlink"):
                dlstbx.util.symlink.create_parent_symlink(
                    final_directory, params.get("create_symlink")
                )

        with open(working_directory / "LIG.smi", "w") as smi_file:
            smi_file.write(smiles)

        pipeline_directory = working_directory / "pipeline_1"

        # if pipeline == "phenix":
        #     phenix_command = f"phenix.ligandfit data={mtz}  model={pdb} ligand=LIG.smi min_ligand_cc_keep={min_cc_keep} nproc=8"  # ligand={ligand_code}
        if pipeline == "phenix_pipeline":
            phenix_command = f"phenix.ligand_pipeline {pdb} {mtz} LIG.smi min_ligand_cc_keep={min_cc_keep} nproc=8"  # build=False

        try:
            result = subprocess.run(
                phenix_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=working_directory,
                check=True,
                timeout=params.get("timeout-minutes") * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(f"Ligand_fit process '{phenix_command}' failed")
            self.log.info(e.stdout)
            self.log.error(e.stderr)
            self.log.info("Sending log to ISPyB")
            self.send_attachments_to_ispyb(pipeline_directory)
            return False

        CC = self.pull_CC_from_log(pipeline_directory)

        if CC >= min_cc_keep:
            os.system(
                f"phenix.mtz2map {pipeline_directory / 'LIG_final.mtz'} {pipeline_directory / 'LIG_final.pdb'} directory={pipeline_directory} selection='resname LIG' buffer=3.5 labels=2FOFCWT,PH2FOFCWT"
            )
            out_map = str(pipeline_directory / "LIG_final_2mFo-DFc.ccp4")
            out_pdb = str(pipeline_directory / "LIG_final.pdb")
            acr = params.get("acronym", "Protein")

            os.system(
                f"module load molviewspec; gen_html_ligandfit.py --pdb_file {out_pdb} --map_file {out_map} --cc {CC} --outdir {pipeline_directory} --smiles '{smiles}' --acr {acr}"
            )

        # self.generate_smiles_png(smiles, pipeline_directory)
        # self.generate_html_visualisation(out_pdb, out_map, pipeline_directory, cc=CC, smiles=smiles, acr=acr)

        data = [
            ["Ligand_fit pipeline", "SMILES code", "Fitting CC"],
            ["phenix.ligand_pipeline", f"{smiles}", f"{CC}"],
        ]

        with open(pipeline_directory / "ligandfit_results.json", "w") as f:
            json.dump(data, f)

        shutil.copytree(
            working_directory,
            results_directory,
            dirs_exist_ok=True,
            ignore=ignore_patterns(".*"),
        )

        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(working_directory), params["create_symlink"]
            )
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(results_directory), params["create_symlink"]
            )

        self.log.info("Sending results to ISPyB")
        self.send_attachments_to_ispyb(pipeline_directory, final_directory)

        if CC >= min_cc_keep:
            self.log.info("Ligand_fitting pipeline finished successfully")
            return True
        else:
            self.log.info(
                f"Ligand_fitting pipeline finished but ligand fitting CC ({CC}) did not meet quality threshold ({min_cc_keep})"
            )
            return False

    # def generate_smiles_png(self, smiles, outdir):
    #     mol = pybel.readstring("smi", smiles)
    #     mol.make2D()
    #     mol.draw(show=False, filename=(f"{outdir}/SMILES.png"))
