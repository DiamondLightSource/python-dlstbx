from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
from pathlib import Path

import gemmi
import numpy as np
import yaml

from dlstbx.wrapper import Wrapper


class PanDDAWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.pandda_xchem"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.info(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        slurm_task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
        params = self.recwrap.recipe_step["job_parameters"]

        PANDDA_2_DIR = "/dls_sw/i04-1/software/PanDDA2"
        # database_path = Path(params.get("database_path"))
        processed_dir = Path(params.get("processed_directory"))
        analysis_dir = Path(processed_dir / "analysis")
        model_dir = Path(params.get("model_directory"))
        auto_panddas_dir = Path(analysis_dir / "auto_pandda2")
        Path(auto_panddas_dir).mkdir(exist_ok=True)

        n_datasets = int(params.get("n_datasets"))
        if n_datasets > 1:  # array job case
            with open(model_dir / ".batch.json", "r") as f:
                datasets = json.load(f)
                dtag = datasets[int(slurm_task_id) - 1]
        else:
            dtag = params.get("dtag")

        self.log.info(f"Processing dtag: {dtag}")
        dataset_dir = model_dir / dtag
        compound_dir = dataset_dir / "compound"

        smiles_files = list(compound_dir.glob("*.smiles"))

        if len(smiles_files) == 0:
            self.log.error(
                f"No .smiles file present in {compound_dir}, cannot continue for dtag {dtag}"
            )
            return False
        elif len(smiles_files) > 1:
            self.log.error(
                f"Multiple .smiles files found in in {compound_dir}:, {smiles_files}, warning for dtag {dtag}"
            )
            return False

        smiles_file = smiles_files[0]
        CompoundCode = smiles_file.stem

        # -------------------------------------------------------
        # acedrg_command = f"module load ccp4; acedrg -i {smiles_file} -o {CompoundCode}"
        restraints_command = f"module load buster; module load graphviz; \
                               export CSDHOME=/dls_sw/apps/CSDS/2024.1.0/; export BDG_TOOL_MOGUL=/dls_sw/apps/CSDS/2024.1.0/ccdc-software/mogul/bin/mogul; \
                               grade2 --in {smiles_file} --itype smi --out {CompoundCode} -f; "

        try:
            result = subprocess.run(
                restraints_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=compound_dir,
                check=True,
                timeout=params.get("timeout-minutes") * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(
                f"Ligand restraint generation command: '{restraints_command}' failed for dataset {dtag}"
            )

            self.log.info(e.stdout)
            self.log.error(e.stderr)
            return False

        restraints = compound_dir / f"{CompoundCode}.restraints.cif"
        restraints.rename(compound_dir / f"{CompoundCode}.cif")
        pdb = compound_dir / f"{CompoundCode}.xyz.pdb"
        pdb.rename(compound_dir / f"{CompoundCode}.pdb")

        with open(dataset_dir / "restraints.log", "w") as log_file:
            log_file.write(result.stdout)

        self.log.info(f"Restraints generated succesfully for dtag {dtag}")

        pandda2_command = f"source /dls_sw/i04-1/software/PanDDA2/venv/bin/activate; \
        python -u /dls_sw/i04-1/software/PanDDA2/scripts/process_dataset.py --data_dirs={model_dir} --out_dir={auto_panddas_dir} --dtag={dtag} --use_ligand_data=False --local_cpus=1"

        try:
            result = subprocess.run(
                pandda2_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=dataset_dir,
                check=True,
                timeout=params.get("timeout-minutes") * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(f"PanDDA2 command: '{pandda2_command}' failed")
            self.log.info(e.stdout)
            self.log.error(e.stderr)
            return False

        dataset_pdir = auto_panddas_dir / "processed_datasets" / dtag
        ligand_dir = dataset_pdir / "ligand_files"

        pandda_log = dataset_pdir / "pandda2.log"
        with open(pandda_log, "w") as log_file:
            log_file.write(result.stdout)

        for item in compound_dir.iterdir():
            if item.is_file():
                shutil.copy2(item, ligand_dir / item.name)

        modelled_dir = dataset_pdir / "modelled_structures"
        out_dir = modelled_dir / "rhofit"
        out_dir.mkdir(parents=True, exist_ok=True)
        event_yaml = dataset_pdir / "events.yaml"

        with open(event_yaml, "r") as file:
            data = yaml.load(file, Loader=yaml.SafeLoader)

        if not data:
            self.log.info(
                (f"No events in {event_yaml}, can't continue with PanDDA2 Rhofit")
            )
            return True  # False

        # Determine which builds to perform. More than one binder is unlikely and score ranks
        # well so build the best scoring event of each dataset.
        best_key = max(data, key=lambda k: data[k]["Score"])
        best_entry = data[best_key]

        # event_idx = best_key
        # bdc = best_entry["BDC"]
        coord = best_entry["Centroid"]

        build_dmap = dataset_pdir / f"{dtag}-z_map.native.ccp4"
        restricted_build_dmap = dataset_pdir / "build.ccp4"
        pdb_file = dataset_pdir / f"{dtag}-pandda-input.pdb"
        mtz_file = dataset_pdir / f"{dtag}-pandda-input.mtz"
        restricted_pdb_file = dataset_pdir / "build.pdb"

        dmap_cut = 2.0
        # This is usually quite a good contour for building and consistent
        # (usually) with the cutoffs PanDDA 2 uses for event finding

        # Rhofit can be confused by hunting non-binding site density. This can be avoided
        # by truncating the map to near the binding site
        dmap = self.read_pandda_map(build_dmap)
        dmap = self.mask_map(dmap, coord)
        self.save_xmap(dmap, restricted_build_dmap)

        # Rhofit masks the protein before building. If the original protein
        # model clips the event then this results in autobuilding becoming impossible.
        # To address tis residues within a 10A neighbourhood of the binding event
        # are removed.
        self.log.debug("Removing nearby atoms to make room for autobuilding")
        self.remove_nearby_atoms(
            pdb_file,
            coord,
            10.0,
            restricted_pdb_file,
        )

        # Really all the cifs should be tried and the best used, or it should try the best
        # cif from PanDDA
        # This is a temporary fix that will get 90% of situations that can be improved upon
        cifs = list(ligand_dir.glob("*.cif"))
        if len(cifs) == 0:
            self.log.error(
                f"No .cif files found for dtag {dtag}, cannot launch PanDDA2 Rhofit!"
            )
            return True

        # -------------------------------------------------------
        rhofit_command = f"module load buster; source {PANDDA_2_DIR}/venv/bin/activate; \
        {PANDDA_2_DIR}/scripts/pandda_rhofit.sh -pdb {restricted_pdb_file} -map {build_dmap} -mtz {mtz_file} -cif {cifs[0]} -out {out_dir} -cut {dmap_cut}; "

        self.log.info(f"Running PanDDA Rhofit command: {rhofit_command}")

        try:
            result = subprocess.run(
                rhofit_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=auto_panddas_dir,
                check=True,
                timeout=params.get("timeout-minutes") * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(f"Rhofit command: '{rhofit_command}' failed")
            self.log.info(e.stdout)
            self.log.error(e.stderr)
            return False

        with open(out_dir / "rhofit.log", "w") as log_file:
            log_file.write(result.stdout)

        # -------------------------------------------------------
        # Merge the protein structure with ligand
        protein_st_file = dataset_pdir / f"{dtag}-pandda-input.pdb"
        ligand_st_file = out_dir / "rhofit" / "best.pdb"
        output_file = modelled_dir / f"{dtag}-pandda-model.pdb"

        protein_st = gemmi.read_structure(str(protein_st_file))
        ligand_st = gemmi.read_structure(str(ligand_st_file))
        contact_chain = self.get_contact_chain(protein_st, ligand_st)
        protein_st[0][contact_chain].add_residue(ligand_st[0][0][0])

        if output_file.exists():
            shutil.copy(output_file, modelled_dir / "pandda-internal-fitted.pdb")

        protein_st.write_pdb(str(output_file))

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

    def remove_nearby_atoms(self, pdb_file, coord, radius, output_file):
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
        new_st.write_pdb(str(output_file))

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

    def update_data_source(self, db_dict, dtag, database_path):
        sql = (
            "UPDATE mainTable SET "
            + ", ".join([f"{k} = :{k}" for k in db_dict])
            + f" WHERE CrystalName = '{dtag}'"
        )
        conn = sqlite3.connect(database_path)
        # conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        cursor.execute(sql, db_dict)
        conn.commit()

    # Integrate back with XCE via datasource
    # db_dict = {}
    # db_dict["DimplePANDDAwasRun"] = True
    # # db_dict["DimplePANDDAreject"] = False
    # db_dict["DimplePANDDApath"] = str(auto_panddas_dir / "processed_datasets")

    # try:
    #     self.update_data_source(db_dict, dtag, database_path)
    #     self.log.info(f"Updated sqlite database for dataset {dtag}")
    # except Exception as e:
    #     self.log.info(f"Could not update sqlite database for dataset {dtag}: {e}")
