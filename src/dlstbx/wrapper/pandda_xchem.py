from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import gemmi
import yaml

from dlstbx.util.mvs.helpers import (
    find_residue_by_name,
    save_cropped_map,
)
from dlstbx.util.mvs.viewer_pandda import gen_html_pandda
from dlstbx.util.pandda import (
    get_contact_chain,
    get_pandda_settings,
    map_sigma,
    mask_map,
    merge_build,
    read_pandda_map,
    remove_nearby_atoms,
    remove_waters_from_ligand,
    save_xmap,
)
from dlstbx.wrapper import Wrapper


class PanDDAWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.pandda_xchem"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.info(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        PANDDA_2_DIR = "/dls_sw/i04-1/software/PanDDA2"
        slurm_task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
        params = self.recwrap.recipe_step["job_parameters"]

        # database_path = Path(params.get("database_path"))
        xchem_visit_dir = Path(params.get("xchem_visit_dir"))
        user_yaml = xchem_visit_dir / ".user.yaml"
        processing_dir = Path(params.get("processing_directory"))
        auto_dir = processing_dir / "auto"
        analysis_dir = Path(auto_dir / "analysis")
        pandda_dir = analysis_dir / "pandda2"
        model_dir = analysis_dir / "model_building"
        panddas_dir = Path(pandda_dir / "panddas")
        Path(panddas_dir).mkdir(parents=True, exist_ok=True)

        overwrite = params.get("overwrite")
        n_datasets = int(params.get("n_datasets") or 1)

        if n_datasets > 1:  # array job case
            batch = True
            with open(model_dir / ".batch.json", "r") as f:
                datasets = json.load(f)
                dtag = datasets[int(slurm_task_id) - 1]
        else:
            dtag = params.get("dtag")
            batch = False

        dataset_dir = model_dir / dtag
        compound_dir = dataset_dir / "compound"

        self.log.info(f"Processing dtag: {dtag}")

        smiles_files = list(compound_dir.glob("*.smiles"))

        if len(smiles_files) == 0:
            self.log.error(
                f"No .smiles file present in {compound_dir}, cannot continue for dtag {dtag}"
            )
            return False
        elif len(smiles_files) > 1:
            self.log.error(
                f"Multiple .smiles files found in in {compound_dir}: {smiles_files}, warning for dtag {dtag}"
            )
            return False

        smiles_file = smiles_files[0]
        CompoundCode = smiles_file.stem
        smiles = smiles_file.read_text().strip()

        # Restraints (grade2) were generated upstream by the ligand-restraints job.
        ligand_cif = compound_dir / f"{CompoundCode}.cif"
        attachments = []

        # -------------------------------------------------------
        # PanDDA2

        dataset_pdir = panddas_dir / "processed_datasets" / dtag
        pandda2_log = dataset_pdir / "pandda2.log"
        attachments.extend([pandda2_log, ligand_cif])

        if overwrite and dataset_pdir.exists():
            shutil.rmtree(dataset_pdir)

        # add any user specified pandda parameters
        args_string = get_pandda_settings(user_yaml)
        pandda2_command = f"source {PANDDA_2_DIR}/venv/bin/activate; \
        python -u /dls_sw/i04-1/software/PanDDA2/scripts/process_dataset.py --data_dirs={model_dir} --out_dir={panddas_dir} --dtag={dtag} --use_ligand_data=True --local_cpus=4 {args_string}"

        self.log.info(f"Running PanDDA2 command: {pandda2_command}")

        try:
            result = subprocess.run(
                pandda2_command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=dataset_dir,
                check=True,
                timeout=params.get("timeout-minutes") * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(f"PanDDA2 command: '{pandda2_command}' failed")
            self.log.info(e.stdout)
            with open(pandda2_log, "w") as log_file:
                log_file.write(e.stdout)
            self.send_attachments_to_ispyb(attachments, batch)
            return False

        with open(pandda2_log, "w") as log_file:
            log_file.write(result.stdout)

        # -------------------------------------------------------
        # PanDDA Rhofit ligand fitting

        ligand_dir = dataset_pdir / "ligand_files"
        ligand_dir.mkdir(exist_ok=True)

        # pandda2 not moving files into ligand_dir fix
        for file in compound_dir.rglob("*"):
            if file.is_file() and file.suffix.lower() in {".pdb", ".cif", ".smiles"}:
                targets = [ligand_dir / file.name, dataset_dir / file.name]
                for target in targets:
                    if target.exists() or target.is_symlink():
                        target.unlink()
                    target.symlink_to(file)

        modelled_dir = dataset_pdir / "modelled_structures"
        out_dir = modelled_dir / "rhofit"
        out_dir.mkdir(parents=True, exist_ok=True)
        events_yaml = dataset_pdir / "events.yaml"

        if not events_yaml.exists():
            self.log.info(
                (f"{events_yaml} does not exist, can't continue with PanDDA2 Rhofit")
            )
            self.send_attachments_to_ispyb(attachments, batch)
            return False

        with open(events_yaml, "r") as file:
            data = yaml.load(file, Loader=yaml.SafeLoader)

        if not data:
            self.log.info(
                (f"No events in {events_yaml}, can't continue with PanDDA2 Rhofit")
            )
            self.send_attachments_to_ispyb(attachments, batch)
            return False

        # Determine which builds to perform. More than one binder is unlikely and score ranks
        # well so build the best scoring event of each dataset.
        best_key = max(data, key=lambda k: data[k]["Score"])
        best_entry = data[best_key]

        event_idx = best_key
        # bdc = best_entry["BDC"]
        event_score = best_entry["Score"]
        event_coord = best_entry["Centroid"]

        restricted_build_dmap = dataset_pdir / "build.ccp4"
        z_map = dataset_pdir / f"{dtag}-z_map.native.ccp4"
        event_map = next(dataset_pdir.glob(f"{dtag}-event_{event_idx}_1-BDC_*"), None)
        pdb_file = dataset_pdir / f"{dtag}-pandda-input.pdb"
        mtz_file = dataset_pdir / f"{dtag}-pandda-input.mtz"
        restricted_pdb_file = dataset_pdir / "build.pdb"

        # Rhofit can be confused by hunting non-binding site density. This can be avoided
        # by truncating the map to near the binding site
        dmap = read_pandda_map(event_map)
        dmap = mask_map(dmap, event_coord)
        save_xmap(dmap, restricted_build_dmap)

        # Rhofit masks the protein before building. If the original protein
        # model clips the event then this results in autobuilding becoming impossible.
        remove_nearby_atoms(
            pdb_file,
            event_coord,
            10.0,
            restricted_pdb_file,
        )

        cifs = list(ligand_dir.glob("*.cif"))
        cut = map_sigma(restricted_build_dmap)

        rhofit_log = dataset_pdir / "rhofit.log"
        attachments.extend([event_map, z_map, rhofit_log])
        rhofit_command = f"module load buster; source {PANDDA_2_DIR}/venv/bin/activate; \
        {PANDDA_2_DIR}/scripts/pandda_rhofit.sh -pdb {restricted_pdb_file} -map {restricted_build_dmap} -mtz {mtz_file} -cif {cifs[0]} -out {out_dir} -cut {cut} > {rhofit_log};"

        self.log.info(f"Running PanDDA Rhofit command: {rhofit_command}")

        try:
            subprocess.run(
                rhofit_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=panddas_dir,
                check=True,
                timeout=60 * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(f"Rhofit command: '{rhofit_command}' failed")
            self.log.info(e.stdout)
            self.log.error(e.stderr)
            self.send_attachments_to_ispyb(attachments, batch)
            return True

        # -------------------------------------------------------
        # Ligand scoring
        build_scores = {}
        build_dir = out_dir / "rhofit"
        rhofit_builds = list(build_dir.glob("Hit*.pdb"))

        # Include any PanDDA2 internal autobuilds
        pandda2_build = next(
            dataset_pdir.glob(f"*_event_{best_key}_best_autobuild.pdb"), None
        )
        if pandda2_build:
            build_scores[pandda2_build] = event_score

        if not rhofit_builds and not pandda2_build:
            self.log.info(f"No autobuilds for {dtag}, can't continue")
            return False

        self.log.info(f"Running Ligand Score routine for {build_dir}")

        # Iterate over rhofit builds and score each one
        for build_path in rhofit_builds:
            ligand_score = build_dir / f"{build_path.stem}.txt"

            st = gemmi.read_structure(str(build_path))
            chain, res = find_residue_by_name(st, "LIG")
            ligand_id = chain.name + f"/{res.seqid.num}"

            score_command = f"source {PANDDA_2_DIR}/venv/bin/activate; \
            python {PANDDA_2_DIR}/scripts/ligand_score.py --mtz_path={mtz_file} --zmap_path={z_map} --ligand_id={ligand_id} --structure_path={build_path} --out_path={ligand_score}"

            try:
                os.system(score_command)

            except Exception as e:
                self.log.error(f"Ligand score command: '{score_command}' failed")
                self.log.info(e.stdout)
                self.log.error(e.stderr)

            with open(ligand_score, "r") as file:
                build_scores[build_path] = float(file.read().strip())

        best_build_path = max(build_scores, key=lambda _x: build_scores[_x])
        best_score = build_scores[best_build_path]

        self.log.info(f"Best ligand score for {dtag} = {best_score}: {best_build_path}")

        # Persist the best score so xchem_collate can bucket datasets by score
        (dataset_pdir / "best_score.txt").write_text(str(best_score))

        # -------------------------------------------------------
        # Merge the protein structure with best fitted ligand -> pandda model

        protein_st_file = dataset_pdir / f"{dtag}-pandda-input.pdb"
        ligand_st_file = best_build_path
        pandda_model = modelled_dir / f"{dtag}-pandda-model.pdb"
        attachments.extend([pandda_model])

        protein_st = gemmi.read_structure(str(protein_st_file))
        ligand_st = gemmi.read_structure(str(ligand_st_file))
        contact_chain = get_contact_chain(protein_st, ligand_st)
        merge_build(protein_st, ligand_st, contact_chain)

        if pandda_model.exists():  # backup previous model
            shutil.copy2(pandda_model, modelled_dir / "pandda-internal-fitted.pdb")

        protein_st.write_pdb(str(pandda_model))

        try:
            remove_waters_from_ligand(pandda_model, self.log)
        except Exception as e:
            self.log.error(f"Exception removing waters from {pandda_model}: {e}")

        # -------------------------------------------------------
        # Output

        try:
            cropped_event_map = save_cropped_map(
                str(pandda_model), str(event_map), "LIG", radius=6
            )
            cropped_z_map = save_cropped_map(
                str(pandda_model), str(z_map), "LIG", radius=6
            )
            mvs_html = gen_html_pandda(
                str(pandda_model),
                cropped_event_map,
                cropped_z_map,
                resname="LIG",
                outdir=dataset_pdir,
                dtag=dtag,
                smiles=smiles,
                score=best_score,
            )
            attachments.extend([mvs_html])
        except Exception as e:
            self.log.debug(f"Exception generating mvs html: {e}")

        data = [
            ["SMILES code", "Best autobuild", "Ligand score"],
            [f"{smiles}", f"{best_build_path}", f"{best_score}"],
        ]
        json_results = dataset_pdir / "pandda2_results.json"
        with open(json_results, "w") as f:
            json.dump(data, f)
        attachments.extend([json_results])

        self.log.info(f"Attachments list: {attachments}")
        self.send_attachments_to_ispyb(attachments, batch)

        self.log.info(f"Auto PanDDA2 pipeline finished successfully for dtag {dtag}")
        return True

    def send_attachments_to_ispyb(self, attachments, batch):
        if batch:  # synchweb attachments not supported for array job processing
            return
        for f in attachments:
            if f.exists():
                if f.suffix == ".html":
                    file_type = "Result"  # 'Graph', 'Debug'
                    importance_rank = 1
                elif f.suffix == ".ccp4":
                    file_type = "Result"
                    importance_rank = 1
                elif f.suffix == ".cif":
                    file_type = "Result"
                    importance_rank = 1
                elif f.suffix == ".pdb":
                    file_type = "Result"
                    importance_rank = 1
                elif f.suffix == ".log":
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
                    self.log.warning(
                        f"Could not attach {f.name} to ISPyB", exc_info=True
                    )
