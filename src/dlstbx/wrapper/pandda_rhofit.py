from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import gemmi
import numpy as np
import yaml

from dlstbx.wrapper import Wrapper


class PanDDARhofitWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.pandda_rhofit"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.info(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        params = self.recwrap.recipe_step["job_parameters"]
        slurm_task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
        # self.log.info((f"SLURM_ARRAY_TASK_ID: {slurm_task_id}"))
        datasets = json.loads(params.get("datasets"))

        # EVENT_MAP_PATTERN = "{dtag}-event_{event_idx}_1-BDC_{bdc}_map.native.ccp4"
        # GROUND_STATE_PATTERN = "{dtag}-ground-state-average-map.native.ccp4"
        PANDDA_2_DIR = "/dls_sw/i04-1/software/PanDDA2"

        processing_dir = Path(params.get("processing_directory"))
        analysis_dir = processing_dir / "analysis"
        model_dir = analysis_dir / "auto_model_building"
        auto_panddas_dir = analysis_dir / "auto_pandda2"

        n_datasets = int(params.get("n_datasets"))
        self.log.info(f"N_datasets: {n_datasets}")
        if n_datasets > 1:
            with open(model_dir / ".batch.json", "r") as f:
                datasets = json.load(f)
                dtag = datasets[int(slurm_task_id) - 1]
        else:
            dtag = params.get("dtag")

        dataset_dir = auto_panddas_dir / "processed_datasets" / dtag
        modelled_dir = dataset_dir / "modelled_structures"
        out_dir = modelled_dir / "rhofit"
        out_dir.mkdir(parents=True, exist_ok=True)

        self.log.info(f"Processing dtag: {dtag}")
        # -------------------------------------------------------

        event_yaml = dataset_dir / "events.yaml"

        with open(event_yaml, "r") as file:
            data = yaml.load(file, Loader=yaml.SafeLoader)

        if not data:
            self.log.info(
                (f"No events in {event_yaml}, can't continue with PanDDA2 Rhofit")
            )
            return False

        # Determine which builds to perform. More than one binder is unlikely and score ranks
        # well so build the best scoring event of each dataset.
        best_key = max(data, key=lambda k: data[k]["Score"])
        best_entry = data[best_key]

        # event_idx = best_key
        # bdc = best_entry["BDC"]
        coord = best_entry["Centroid"]

        dataset_dir = auto_panddas_dir / "processed_datasets" / dtag
        ligand_dir = dataset_dir / "ligand_files"
        build_dmap = dataset_dir / f"{dtag}-z_map.native.ccp4"
        restricted_build_dmap = dataset_dir / "build.ccp4"
        pdb_file = dataset_dir / f"{dtag}-pandda-input.pdb"
        mtz_file = dataset_dir / f"{dtag}-pandda-input.mtz"
        restricted_pdb_file = dataset_dir / "build.pdb"

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
        cifs = [x for x in ligand_dir.glob("*.cif")]
        if len(cifs) == 0:
            self.log.error("No .cif files found!")

        # -------------------------------------------------------
        rhofit_command = f"module load buster; source {PANDDA_2_DIR}/venv/bin/activate; \
        {PANDDA_2_DIR}/scripts/pandda_rhofit.sh -pdb {restricted_pdb_file} -map {build_dmap} -mtz {mtz_file} -cif {cifs[0]} -out {out_dir} -cut {dmap_cut}; "
        # cp {modelled_dir}/{dtag}-pandda-model.pdb {modelled_dir}/pandda-internal-fitted.pdb;

        self.log.info("Running rhofit command: {rhofit_command}")

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
        protein_st_file = dataset_dir / f"{dtag}-pandda-input.pdb"
        ligand_st_file = out_dir / "rhofit" / "best.pdb"
        output_file = modelled_dir / f"{dtag}-pandda-model.pdb"

        protein_st = gemmi.read_structure(str(protein_st_file))
        ligand_st = gemmi.read_structure(str(ligand_st_file))
        contact_chain = self.get_contact_chain(protein_st, ligand_st)
        protein_st[0][contact_chain].add_residue(ligand_st[0][0][0])

        protein_st.write_pdb(str(output_file))

        self.log.info("Auto PanDDA2-Rhofit finished successfully")
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
