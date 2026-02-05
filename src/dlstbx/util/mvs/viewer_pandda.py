from __future__ import annotations

from pathlib import Path

import gemmi
import molviewspec as mvs

from dlstbx.util.mvs.helpers import find_residue_by_name


def gen_html_pandda(pdb_file, event_map, z_map, resname, outdir, dtag, smiles):
    # make an mvs story from snapshots
    st = gemmi.read_structure(pdb_file)
    chain, res = find_residue_by_name(st, resname)
    residue = mvs.ComponentExpression(label_seq_id=res.seqid.num)

    builder = mvs.create_builder()
    structure = builder.download(url=pdb_file).parse(format="pdb").model_structure()
    structure.component(selector="polymer").representation(
        type="surface", size_factor=0.7
    ).opacity(opacity=0.2).color(color="#AABDF1")
    structure.component(selector="polymer").representation().opacity(
        opacity=0.25
    ).color(custom={"molstar_color_theme_name": "chain_id"})

    structure.component(selector=residue).focus().representation(
        type="ball_and_stick"
    ).color(custom={"molstar_color_theme_name": "element-symbol"})

    ccp4 = builder.download(url=event_map).parse(format="map")
    ccp4.volume().representation(
        type="isosurface",
        relative_isovalue=3,
        show_wireframe=True,
        show_faces=False,
    ).color(color="blue").opacity(opacity=0.25)

    structure.component(
        selector=residue,
        custom={
            "molstar_show_non_covalent_interactions": True,
            "molstar_non_covalent_interactions_radius_ang": 5,
        },
    )

    snapshot1 = builder.get_snapshot(
        title="Eventmap",
        description=f"## PanDDA2 Results: \n ### {dtag} \n - SMILES: {smiles} \n - Event map at 2σ, blue",
        transition_duration_ms=700,
        linger_duration_ms=4000,
    )

    # SNAPSHOT2
    builder = mvs.create_builder()
    structure = builder.download(url=pdb_file).parse(format="pdb").model_structure()
    structure.component(selector="polymer").representation(
        type="surface", size_factor=0.7
    ).opacity(opacity=0.2).color(color="#AABDF1")
    structure.component(selector="polymer").representation().opacity(
        opacity=0.25
    ).color(custom={"molstar_color_theme_name": "chain_id"})
    structure.component(selector="ligand").representation(
        type="surface", size_factor=0.7
    ).opacity(opacity=0.1).color(custom={"molstar_color_theme_name": "element-symbol"})

    structure.component(selector=residue).focus().representation(
        type="ball_and_stick"
    ).color(custom={"molstar_color_theme_name": "element-symbol"})

    ccp4 = builder.download(url=z_map).parse(format="map")
    ccp4.volume().representation(
        type="isosurface",
        relative_isovalue=3,
        show_wireframe=True,
        show_faces=False,
    ).color(color="green").opacity(opacity=0.25)

    structure.component(
        selector=residue,
        custom={
            "molstar_show_non_covalent_interactions": True,
            "molstar_non_covalent_interactions_radius_ang": 5,
        },
    )

    snapshot2 = builder.get_snapshot(
        title="Z_map",
        description=f"## PanDDA2 Results: \n ### {dtag} \n - SMILES: {smiles} \n - Z_map at 3σ, green",
        transition_duration_ms=700,
        linger_duration_ms=4000,
    )

    states = mvs.States(
        snapshots=[snapshot2],  # [snapshot1, snapshot2]
        metadata=mvs.GlobalMetadata(description="PanDDA2 Results"),
    )

    with open(pdb_file) as f:
        pdb_data = f.read()

    # with open(event_map, mode="rb") as f:
    #     map_data1 = f.read()

    with open(z_map, mode="rb") as f:
        map_data2 = f.read()

    html = mvs.molstar_html(
        states,
        data={pdb_file: pdb_data, z_map: map_data2},  # event_map: map_data1,
        ui="stories",
    )

    out_file = Path(f"{outdir}/pandda2.html")
    with open(out_file, "w") as f:
        f.write(html)

    return out_file
