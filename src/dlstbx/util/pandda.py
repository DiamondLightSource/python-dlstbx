from __future__ import annotations

import gemmi
import numpy as np
import yaml

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


def save_xmap(xmap, xmap_file):
    """Convenience script for saving ccp4 files."""
    ccp4 = gemmi.Ccp4Map()
    ccp4.grid = xmap
    ccp4.update_ccp4_header()
    ccp4.write_ccp4_map(str(xmap_file))


def read_pandda_map(xmap_file):
    """PanDDA 2 maps are often truncated, and PanDDA 1 maps can have misasigned
    spacegroups. This method handles both."""
    dmap_ccp4 = gemmi.read_ccp4_map(str(xmap_file), setup=False)
    dmap_ccp4.grid.spacegroup = gemmi.find_spacegroup_by_name("P1")
    dmap_ccp4.setup(0.0)
    dmap = dmap_ccp4.grid
    return dmap


def map_sigma(xmap, sigma=1):
    ccp4 = gemmi.read_ccp4_map(str(xmap))
    ccp4.setup(0.0)
    grid = ccp4.grid
    grid_array = np.array(grid, copy=False)
    non_zero_std = np.std(grid_array[(grid_array < -0.05) | (grid_array > 0.05)])
    return non_zero_std * sigma


def mask_map(dmap, coord, radius=10.0):
    """Simple routine to mask density to region around a specified point."""
    mask = gemmi.FloatGrid(dmap.nu, dmap.nv, dmap.nw)
    mask.set_unit_cell(dmap.unit_cell)
    mask.set_points_around(
        gemmi.Position(coord[0], coord[1], coord[2]), radius=radius, value=1.0
    )

    dmap_array = np.array(dmap, copy=False)
    dmap_array[:, :, :] = dmap_array[:, :, :] * np.array(mask)[:, :, :]

    return dmap


def remove_nearby_atoms(pdb_file, coord, radius, pandda_model):
    """Remove every residue with an atom within `radius` of `coord` and write the
    truncated structure to `pandda_model`."""
    st = gemmi.read_structure(str(pdb_file))
    target = gemmi.Position(coord[0], coord[1], coord[2])
    for model in st:
        for chain in model:
            # Iterate back-to-front so deleting index i doesn't shift the
            # indices we have yet to visit.
            for i in reversed(range(len(chain))):
                if any(atom.pos.dist(target) < radius for atom in chain[i]):
                    del chain[i]
    st.write_pdb(str(pandda_model))


def remove_waters_from_ligand(pandda_model, logger=None):
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

    st.write_pdb(str(pandda_model))
    if logger:
        logger.info(f"Removed {len(waters_to_remove)} waters in {pandda_model.name}")


def get_contact_chain(protein_st, ligand_st):
    """A simple estimation of the contact chain based on which protein chain has
    the most atoms near the ligand centroid."""
    ligand_pos_list = []
    for model in ligand_st:
        for chain in model:
            for res in chain:
                for atom in res:
                    pos = atom.pos
                    ligand_pos_list.append([pos.x, pos.y, pos.z])
    centroid = np.mean(np.array(ligand_pos_list), axis=0)

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

    return max(chain_counts, key=lambda _x: chain_counts[_x])


def merge_build(receptor, ligand, contact_chain):
    # Get the receptor chain
    receptor_chain = receptor[0][contact_chain]

    # Get current ligand ids
    seqid_nums = []
    for receptor_res in receptor_chain:
        num = receptor_res.seqid.num
        seqid_nums.append(num)

    # Assign a new, unused ligand id
    if len(seqid_nums) == 0:
        min_ligand_seqid = 100
    else:
        min_ligand_seqid = max(seqid_nums) + 100

    # Update the ligand residue sequenceid
    ligand_residue = ligand[0][0][0]
    ligand_residue.seqid.num = min_ligand_seqid

    # Add the ligand residue
    receptor_chain.add_residue(ligand_residue, pos=-1)

    return receptor


def get_pandda_settings(yaml_file):
    with open(yaml_file, "r") as file:
        expt_yaml = yaml.load(file, Loader=yaml.SafeLoader)
    settings = expt_yaml.get("autoprocessing", {}).get("pandda", {})
    if settings:
        args_string = " ".join(f"--{k}={v}" for k, v in settings.items())
    else:
        args_string = ""
    return args_string
