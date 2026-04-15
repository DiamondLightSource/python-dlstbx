from __future__ import annotations

from pathlib import Path

import gemmi


def find_residue_by_name(structure, name):
    for model in structure:
        for chain in model:
            for res in chain:
                if res.name == name:
                    return chain, res
    raise ValueError(f"Residue {name} not found")


def residue_centroid(residue):
    n = 0
    x = y = z = 0.0
    for at in residue:
        p = at.pos
        x += p.x
        y += p.y
        z += p.z
        n += 1
    if n == 0:
        raise ValueError("Residue has no atoms")
    return gemmi.Position(x / n, y / n, z / n)


def save_cropped_map(pdb_file, map_file, resname, radius):
    st = gemmi.read_structure(pdb_file)
    cell = st.cell
    m = gemmi.read_ccp4_map(map_file, setup=True)
    grid = m.grid

    chain, res = find_residue_by_name(st, resname)
    center = residue_centroid(res)  # ligand center

    mask = grid.clone()
    mask.fill(0.0)

    mask.set_points_around(center, radius, 1.0, use_pbc=True)  # spherical mask in Å

    dl = gemmi.Position(radius, radius, radius)  # box d/2
    box = gemmi.FractionalBox()
    box.extend(cell.fractionalize(center - dl))
    box.extend(cell.fractionalize(center + dl))

    grid.array[:] *= mask.array
    m.set_extent(box)
    path = Path(map_file)
    map_out = str(path.parents[0] / f"{path.stem}_cropped.ccp4")
    m.write_ccp4_map(map_out)
    return map_out
