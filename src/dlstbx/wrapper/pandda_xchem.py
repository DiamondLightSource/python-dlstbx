from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import gemmi
import numpy as np
import yaml

from dlstbx.util.mvs.helpers import (
    find_residue_by_name,
    save_cropped_map,
)
from dlstbx.util.mvs.viewer_pandda import gen_html_pandda
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
        model_dir = pandda_dir / "model_building"
        panddas_dir = Path(pandda_dir / "panddas")
        Path(panddas_dir).mkdir(exist_ok=True)

        n_datasets = int(params.get("n_datasets"))
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

        smiles = params.get("smiles")
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

        # -------------------------------------------------------
        # Ligand restraint generation

        restraints_log = dataset_dir / "restraints.log"
        attachments = [restraints_log]  # synchweb attachments
        restraints_command = f"grade2 --in {smiles_file} --itype smi --out {CompoundCode} -f > {restraints_log}"
        # acedrg_command = f"module load ccp4; acedrg -i {smiles_file} -o {CompoundCode}"

        try:
            subprocess.run(
                restraints_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=compound_dir,
                check=True,
                timeout=30 * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(
                f"Ligand restraint generation command: '{restraints_command}' failed for dataset {dtag}"
            )

            self.log.info(e.stdout)
            self.log.error(e.stderr)
            self.send_attachments_to_ispyb(attachments, batch)
            return False

        restraints = compound_dir / f"{CompoundCode}.restraints.cif"
        restraints.rename(compound_dir / f"{CompoundCode}.cif")
        ligand_pdb = compound_dir / f"{CompoundCode}.xyz.pdb"
        ligand_pdb.rename(compound_dir / f"{CompoundCode}.pdb")

        ligand_cif = compound_dir / f"{CompoundCode}.cif"

        self.log.info(
            f"Restraints generated succesfully for dtag {dtag}, launching PanDDA2"
        )

        # -------------------------------------------------------
        # PanDDA2

        dataset_pdir = panddas_dir / "processed_datasets" / dtag
        pandda2_log = dataset_pdir / "pandda2.log"
        attachments.extend([pandda2_log, ligand_cif])

        args_string = self.get_pandda_settings(
            user_yaml
        )  # user specified pandda parameters
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

        # pandda2 not moving files into ligand_dir, fix
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
                (f"No events in {events_yaml}, can't continue with PanDDA2 Rhofit")
            )
            self.send_attachments_to_ispyb(attachments, batch)
            return False

        with open(events_yaml, "r") as file:
            data = yaml.load(file, Loader=yaml.SafeLoader)

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
        dmap = self.read_pandda_map(event_map)
        dmap = self.mask_map(dmap, event_coord)
        self.save_xmap(dmap, restricted_build_dmap)

        # Rhofit masks the protein before building. If the original protein
        # model clips the event then this results in autobuilding becoming impossible.
        # To address this residues within a 10A neighbourhood of the binding event
        # are removed.
        self.log.debug("Removing nearby atoms to make room for autobuilding")
        self.remove_nearby_atoms(
            pdb_file,
            event_coord,
            10.0,
            restricted_pdb_file,
        )

        cifs = list(ligand_dir.glob("*.cif"))
        cut = self.map_sigma(restricted_build_dmap)

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
        pandda2_build = (
            next(dataset_pdir.glob(f"*_event_{best_key}_best_autobuild.pdb")),
            None,
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

        self.log.info(f"Best ligand score for {dtag} = {best_score}")

        # -------------------------------------------------------
        # Merge the protein structure with best fitted ligand -> pandda model

        protein_st_file = dataset_pdir / f"{dtag}-pandda-input.pdb"
        ligand_st_file = best_build_path
        pandda_model = modelled_dir / f"{dtag}-pandda-model.pdb"
        attachments.extend([pandda_model])

        protein_st = gemmi.read_structure(str(protein_st_file))
        ligand_st = gemmi.read_structure(str(ligand_st_file))
        contact_chain = self.get_contact_chain(protein_st, ligand_st)
        protein_st[0][contact_chain].add_residue(ligand_st[0][0][0])

        if pandda_model.exists():  # backup previous model
            shutil.copy2(pandda_model, modelled_dir / "pandda-internal-fitted.pdb")

        protein_st.write_pdb(str(pandda_model))

        try:
            self.remove_waters_from_ligand(pandda_model)
        except Exception as e:
            self.log.error(f"{dtag}: {e}")

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

        # data = [
        #     ["SMILES code", "Fitted_ligand?", "Hit?"],
        #     [f"{smiles}", "True", "True"],
        # ]
        # json_results = dataset_pdir / "pandda2_results.json"
        # with open(json_results, "w") as f:
        #     json.dump(data, f)
        # attachments.extend([json_results])

        self.log.info(f"Attachments list: {attachments}")
        self.send_attachments_to_ispyb(attachments, batch)

        self.log.info(f"Auto PanDDA2 pipeline finished successfully for dtag {dtag}")
        return True

    def save_xmap(self, xmap, xmap_file):
        """Convenience script for saving ccp4 files."""
        ccp4 = gemmi.Ccp4Map()
        ccp4.grid = xmap
        ccp4.update_ccp4_header()
        ccp4.write_ccp4_map(str(xmap_file))

    def read_pandda_map(self, xmap_file):
        """PanDDA 2 maps are often truncated, and PanDDA 1 maps can have misasigned spacegroups.
        This method handles both."""
        dmap_ccp4 = gemmi.read_ccp4_map(str(xmap_file), setup=False)
        dmap_ccp4.grid.spacegroup = gemmi.find_spacegroup_by_name("P1")
        dmap_ccp4.setup(0.0)
        dmap = dmap_ccp4.grid
        return dmap

    def map_sigma(self, xmap, sigma=1):
        ccp4 = gemmi.read_ccp4_map(str(xmap))
        ccp4.setup(0.0)
        grid = ccp4.grid
        grid_array = np.array(grid, copy=False)
        non_zero_std = np.std(grid_array[(grid_array < -0.05) | (grid_array > 0.05)])
        return non_zero_std * sigma

    def expand_event_map(self, bdc, ground_state_file, xmap_file, coord, out_file):
        """DEPRECATED. A method for recalculating event maps over the full cell."""
        ground_state_ccp4 = gemmi.read_ccp4_map(str(ground_state_file), setup=False)
        ground_state_ccp4.grid.spacegroup = gemmi.find_spacegroup_by_name("P1")
        ground_state_ccp4.setup(0.0)
        ground_state = ground_state_ccp4.grid

        xmap_ccp4 = gemmi.read_ccp4_map(str(xmap_file), setup=False)
        xmap_ccp4.grid.spacegroup = gemmi.find_spacegroup_by_name("P1")
        xmap_ccp4.setup(0.0)
        xmap = xmap_ccp4.grid

        mask = gemmi.FloatGrid(xmap.nu, xmap.nv, xmap.nw)
        mask.set_unit_cell(xmap.unit_cell)
        mask.set_points_around(
            gemmi.Position(coord[0], coord[1], coord[2]), radius=10.0, value=1.0
        )

        event_map = gemmi.FloatGrid(xmap.nu, xmap.nv, xmap.nw)
        event_map.set_unit_cell(xmap.unit_cell)
        event_map_array = np.array(event_map, copy=False)
        event_map_array[:, :, :] = np.array(xmap)[:, :, :] - (
            bdc * np.array(ground_state)[:, :, :]
        )
        event_map_array[:, :, :] = event_map_array[:, :, :] * np.array(mask)[:, :, :]

        event_map_non_zero = event_map_array[event_map_array != 0.0]
        cut = np.std(event_map_non_zero)

        return cut

    def mask_map(self, dmap, coord, radius=10.0):
        """Simple routine to mask density to region around a specified point."""
        mask = gemmi.FloatGrid(dmap.nu, dmap.nv, dmap.nw)
        mask.set_unit_cell(dmap.unit_cell)
        mask.set_points_around(
            gemmi.Position(coord[0], coord[1], coord[2]), radius=radius, value=1.0
        )

        dmap_array = np.array(dmap, copy=False)
        dmap_array[:, :, :] = dmap_array[:, :, :] * np.array(mask)[:, :, :]

        return dmap

    def remove_nearby_atoms(self, pdb_file, coord, radius, pandda_model):
        """An inelegant method for removing residues near the event centroid and creating
        a new, truncated pdb file. GEMMI doesn't have a super nice way to remove
        residues according to a specific criteria."""
        st = gemmi.read_structure(str(pdb_file))
        new_st = st.clone()  # Clone to keep metadata

        coord_array = np.array([coord[0], coord[1], coord[2]])

        # Delete all residues for a clean chain. Yes this is an arcane way to do it.
        chains_to_delete = []
        for model in st:
            for chain in model:
                chains_to_delete.append((model.num, chain.name))

        for model in new_st:
            for chain in model:
                for res in chain:
                    del chain[-1]

        # Add non-rejected residues to a new structure
        for j, model in enumerate(st):
            for k, chain in enumerate(model):
                for res in chain:
                    add_res = True
                    for atom in res:
                        pos = atom.pos
                        distance = np.linalg.norm(
                            coord_array - np.array([pos.x, pos.y, pos.z])
                        )
                        if distance < radius:
                            add_res = False

                    if add_res:
                        new_st[j][k].add_residue(res)
        new_st.write_pdb(str(pandda_model))

    def remove_waters_from_ligand(self, pandda_model):
        st = gemmi.read_structure(str(pandda_model))
        st.setup_entities()

        LIGAND_RES_NAME = "LIG"

        # Collect ligand atoms with their VdW radii
        ligand_atoms = []  # list of (pos, vdw_radius)
        for chain in st[0]:
            for res in chain:
                if res.name == LIGAND_RES_NAME:
                    for atom in res:
                        vdw = atom.element.vdw_r
                        ligand_atoms.append((atom.pos, vdw))

        max_vdw = max(r for _, r in ligand_atoms)
        search_radius = max_vdw  # + O_VDW=1.5

        ns = gemmi.NeighborSearch(st[0], st.cell, search_radius).populate()

        # Find waters where any atom overlaps within VdW radii
        waters_to_remove = set()
        for lig_pos, lig_vdw in ligand_atoms:
            for mark in ns.find_atoms(lig_pos, "\0", radius=search_radius):
                cra = mark.to_cra(st[0])
                if cra.residue.entity_type != gemmi.EntityType.Water:
                    continue
                wat_vdw = cra.atom.element.vdw_r
                cutoff = lig_vdw + wat_vdw
                dist = lig_pos.dist(mark.pos)
                if dist < cutoff:
                    waters_to_remove.add((cra.chain.name, cra.residue.seqid))

        # Remove waters
        for chain in st[0]:
            to_delete = [
                i
                for i, res in enumerate(chain)
                if res.entity_type == gemmi.EntityType.Water
                and (chain.name, res.seqid) in waters_to_remove
            ]
            for i in reversed(to_delete):  # reversed so indices stay valid
                del chain[i]

        st.write_pdb(str(pandda_model.parents[0] / pandda_model.name))
        self.log.info(f"Removed {len(waters_to_remove)} waters in {pandda_model.name}")

    def get_contact_chain(self, protein_st, ligand_st):
        """A simple estimation of the contact chain based on which chain has the most atoms
        nearby."""
        ligand_pos_list = []
        for model in protein_st:
            for chain in model:
                for res in chain:
                    for atom in res:
                        pos = atom.pos
                        ligand_pos_list.append([pos.x, pos.y, pos.z])
        centroid = np.linalg.norm(np.array(ligand_pos_list), axis=0)

        PROTEIN_RESIDUES = [
            "ALA",
            "ARG",
            "ASN",
            "ASP",
            "CYS",
            "GLN",
            "GLU",
            "HIS",
            "ILE",
            "LEU",
            "LYS",
            "MET",
            "PHE",
            "PRO",
            "SER",
            "THR",
            "TRP",
            "TYR",
            "VAL",
            "GLY",
        ]

        chain_counts = {}
        for model in protein_st:
            for chain in model:
                chain_counts[chain.name] = 0
                for res in chain:
                    if res.name not in PROTEIN_RESIDUES:
                        continue
                    for atom in res:
                        pos = atom.pos
                        distance = np.linalg.norm(
                            np.array([pos.x, pos.y, pos.z]) - centroid
                        )
                        if distance < 5.0:
                            chain_counts[chain.name] += 1

        return min(chain_counts, key=lambda _x: chain_counts[_x])

    def get_pandda_settings(self, yaml_file):
        with open(yaml_file, "r") as file:
            expt_yaml = yaml.load(file, Loader=yaml.SafeLoader)
        settings = expt_yaml.get("autoprocessing", {}).get("pandda", {})
        if settings:
            args_string = " ".join(f"--{k}={v}" for k, v in settings.items())
        else:
            args_string = ""
        return args_string

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
