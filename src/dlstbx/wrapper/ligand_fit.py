from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import subprocess
from shutil import ignore_patterns

# import molviewspec as mvs
from iotbx import pdb

# from rdkit import Chem
# from rdkit.Chem import Draw
import dlstbx.util.symlink
from dlstbx.wrapper import Wrapper


class LigandFitWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.ligand_fit"

    def pull_CC_from_log(self, pipeline_directory):
        f = pipeline_directory / "pipeline.log"
        file_read = open(f, "r")
        text = "Overall CC"
        lines = file_read.readlines()
        llist = []
        for line in lines:
            if text in line:
                llist.append(line)
        file_read.close()
        mystring = llist[0]
        CC = float(re.findall(r"\d+\.\d+", mystring)[0])
        return CC

    def send_attachments_to_ispyb(self, pipeline_directory, min_cc_keep):
        CC = self.pull_CC_from_log(pipeline_directory)
        for f in pipeline_directory.iterdir():
            if f.stem.endswith("final") and CC >= min_cc_keep:
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
        with open(working_directory / "LIG.smi", "w") as smi_file:
            smi_file.write(smiles)

        pipeline_directory = working_directory / "pipeline_1"

        # if pipeline == "phenix":
        #     phenix_command = f"phenix.ligandfit data={mtz}  model={pdb} ligand=LIG.smi min_ligand_cc_keep={min_cc_keep} nproc=8"  # ligand={ligand_code}
        if pipeline == "phenix_pipeline":
            phenix_command = f"phenix.ligand_pipeline {pdb} {mtz} LIG.smi min_ligand_cc_keep={min_cc_keep} nproc=8"

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
            return False

        with open(working_directory / "ligand_fit.log", "w") as log_file:
            log_file.write(result.stdout)

        CC = self.pull_CC_from_log(pipeline_directory)

        if CC >= min_cc_keep:
            os.system(
                f"phenix.mtz2map {pipeline_directory/'LIG_final.mtz'} {pipeline_directory/'LIG_final.pdb'} directory={pipeline_directory} selection='resname LIG' buffer=3.5 labels=2FOFCWT,PH2FOFCWT"
            )
            out_map = str(pipeline_directory / "LIG_final_2mFo-DFc.ccp4")
            out_pdb = str(pipeline_directory / "LIG_final.pdb")
            acr = params.get("acronym", "Protein")

            mvs_command = f"module load molviewspec; \
                            gen_html.py --pdb_file {out_pdb} --map_file {out_map} --cc {CC} --outdir {pipeline_directory} --smiles '{smiles}' --acr {acr}"

            try:
                result = subprocess.run(
                    mvs_command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    cwd=working_directory,
                    check=True,
                )

            except subprocess.CalledProcessError as e:
                self.log.error(f"Ligand_fit process '{mvs_command}' failed")
                self.log.info(e.stdout)
                self.log.error(e.stderr)

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
        self.send_attachments_to_ispyb(pipeline_directory, min_cc_keep)

        if CC >= min_cc_keep:
            self.log.info("Ligand_fitting pipeline finished successfully")
            return True
        else:
            self.log.info(
                f"Ligand_fitting pipeline finished but ligand fitting CC ({CC}) did not meet quality threshold ({min_cc_keep})"
            )
            return False

    # def generate_smiles_png(self, smiles, outdir):
    #     mol = Chem.MolFromSmiles(smiles)
    #     img = Draw.MolToImage(mol, size=(450, 450))
    #     img.save(f"{outdir}/SMILES.png")


# def generate_html_visualisation(pdb_file, map_file, outdir, acr, smiles, cc):
#     # generate html with multiple snapshots
#     builder = mvs.create_builder()
#     structure = builder.download(url=pdb_file).parse(format="pdb").model_structure()
#     structure.component(selector="polymer").representation(
#         type="surface", size_factor=0.7
#     ).opacity(opacity=0.6).color(color="#EEC4EE")
#     structure.component(selector="polymer").representation().opacity(opacity=0.6).color(
#         color="grey"
#     )
#     structure.component(selector="ligand").representation(type="ball_and_stick").color(
#         custom={"molstar_color_theme_name": "element-symbol"}
#     )
#     structure.component(selector="ligand").representation(type="surface").opacity(
#         opacity=0.1
#     ).color(custom={"molstar_color_theme_name": "element-symbol"})

#     ccp4 = builder.download(url=map_file).parse(format="map")
#     ccp4.volume().representation(
#         type="isosurface",
#         relative_isovalue=1.5,
#         show_wireframe=True,
#         show_faces=False,
#     ).color(color="blue").opacity(opacity=0.25)

#     snapshot1 = builder.get_snapshot(
#         title="Main View",
#         description=f"## Ligand_Fit Results: \n ### {acr} with ligand & electron density map \n - SMILES: {smiles} \n - 2FO-FC at 1.5σ, blue \n - Fitting CC = {cc}",
#         transition_duration_ms=2000,
#         linger_duration_ms=5000,
#     )

#     # snapshot 2
#     builder = mvs.create_builder()
#     structure = builder.download(url=pdb_file).parse(format="pdb").model_structure()
#     structure.component(selector="polymer").representation(
#         type="surface", size_factor=0.7
#     ).opacity(opacity=0.5).color(color="#D8BFD8")
#     structure.component(selector="polymer").representation().opacity(opacity=0.6).color(
#         color="grey"
#     )
#     structure.component(selector="ligand").focus().representation(
#         type="ball_and_stick"
#     ).color(custom={"molstar_color_theme_name": "element-symbol"})

#     ccp4 = builder.download(url=map_file).parse(format="map")
#     ccp4.volume().representation(
#         type="isosurface",
#         relative_isovalue=1.5,
#         show_wireframe=True,
#         show_faces=False,
#     ).color(color="blue").opacity(opacity=0.25)

#     # add a label
#     info = get_chain_and_residue_numbers(pdb_file, "LIG")
#     resid = info[0][1]
#     residue = mvs.ComponentExpression(label_seq_id=resid)
#     (
#         structure.component(
#             selector=residue,
#             custom={
#                 "molstar_show_non_covalent_interactions": True,
#                 "molstar_non_covalent_interactions_radius_ang": 5.0,
#             },
#         ).label(text=f"CC = {cc}")
#     )

#     snapshot2 = builder.get_snapshot(
#         title="Focus View",
#         description=f"## Ligand_Fit Results: \n ### {acr} with ligand & electron density map \n - SMILES: {smiles} \n - 2FO-FC at 1.5σ, blue \n - Fitting CC = {cc}",
#         transition_duration_ms=2000,
#         linger_duration_ms=5000,
#     )

#     states = mvs.States(
#         snapshots=[snapshot1, snapshot2],
#         metadata=mvs.GlobalMetadata(description="Ligand_fit Results"),
#     )

#     with open(pdb_file) as f:
#         pdb_data = f.read()

#     with open(map_file, mode="rb") as f:
#         map_data = f.read()

#     html = mvs.molstar_widgets.molstar_html(
#         states,
#         data={pdb_file: pdb_data, map_file: map_data},
#         ui="stories",
#     )

#     with open(outdir / "ligand_fit.html", "w") as f:
#         f.write(html)


def get_chain_and_residue_numbers(pdb_file_path, target_residue_name):
    """
    Finds (chain ID, residue number) for a given residue name in a PDB file.
    """
    pdb_hierarchy = pdb.input(file_name=pdb_file_path).construct_hierarchy()

    results = [
        (res.parent().id.strip(), f"{res.resseq.strip()}{res.icode.strip() or ''}")
        for res in pdb_hierarchy.residue_groups()
        for ag in res.atom_groups()
        if ag.resname.strip() == target_residue_name
    ]

    return results
