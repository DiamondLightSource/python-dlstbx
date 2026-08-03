from __future__ import annotations

from pathlib import Path

import gemmi
import numpy as np


def map_sigma(map_file):
    """RMS of a map over the voxels that actually carry data.

    PanDDA event maps are masked to a small fraction of the grid (~2%), the rest being
    NaN or zero. Contouring at "N sigma" only means something if sigma comes from the
    real voxels -- taken over the whole grid it is dominated by the padding, and two
    maps padded differently end up contoured at incomparable levels.
    """
    grid = gemmi.read_ccp4_map(str(map_file), setup=True).grid
    array = np.asarray(grid, dtype=float)
    values = array[np.isfinite(array) & (array != 0)]
    return float(values.std()) if values.size else float("nan")


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


def save_cropped_map(
    pdb_file,
    map_file,
    resname,
    radius,
    out_dir=None,
    center=None,
    positions=None,
    prefix="",
):
    """Crop a map to the neighbourhood of a ligand.

    ``positions`` masks a small sphere around each of the given points instead of one
    big sphere around the centroid, so the surface hugs the ligand and the view is not
    obscured by density from everything else within the radius. Pass the atom positions
    of every pose being compared, or one pose's density gets clipped away.

    ``out_dir`` writes the cropped map somewhere other than next to the input -- needed
    when the map lives in a directory that must not be written to. ``center`` overrides
    the ligand lookup, for picking one particular copy. ``prefix`` disambiguates the
    output name: two pandda runs of the same dataset produce event maps with identical
    basenames, so cropping both into one directory silently overwrites.
    """
    st = gemmi.read_structure(pdb_file)
    cell = st.cell
    m = gemmi.read_ccp4_map(map_file, setup=True)
    grid = m.grid

    if positions is None:
        if center is None:
            chain, res = find_residue_by_name(st, resname)
            center = residue_centroid(res)  # ligand center
        positions = [center]

    # PanDDA2 writes its out-of-mask voxels as NaN (legacy runs used 0). NaN * 0 is NaN,
    # so without this the "cropped" map stays full of NaN and the viewer's relative
    # isovalue, which is derived from the volume's own statistics, is meaningless.
    grid.array[~np.isfinite(grid.array)] = 0.0

    mask = grid.clone()
    mask.fill(0.0)

    dl = gemmi.Position(radius, radius, radius)  # box d/2
    box = gemmi.FractionalBox()
    for point in positions:
        mask.set_points_around(point, radius, 1.0, use_pbc=True)  # spherical mask in Å
        box.extend(cell.fractionalize(point - dl))
        box.extend(cell.fractionalize(point + dl))

    grid.array[:] *= mask.array
    m.set_extent(box)
    path = Path(map_file)
    map_out = str(Path(out_dir or path.parent) / f"{prefix}{path.stem}_cropped.ccp4")
    m.write_ccp4_map(map_out)
    return map_out
