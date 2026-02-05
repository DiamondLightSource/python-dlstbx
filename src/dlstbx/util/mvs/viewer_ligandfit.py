from __future__ import annotations

import gemmi
import molviewspec as mvs

from dlstbx.util.mvs.helpers import find_residue_by_name


def gen_html_ligandfit(pdb_file, map_file, resname, outdir, acr, smiles, cc):
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
    structure.component(selector="ligand").representation(type="ball_and_stick").color(
        custom={"molstar_color_theme_name": "element-symbol"}
    )
    structure.component(selector="ligand").representation(type="surface").opacity(
        opacity=0.1
    ).color(custom={"molstar_color_theme_name": "element-symbol"})

    ccp4 = builder.download(url=map_file).parse(format="map")
    ccp4.volume().representation(
        type="isosurface",
        relative_isovalue=1.5,
        show_wireframe=True,
        show_faces=False,
    ).color(color="blue").opacity(opacity=0.25)

    snapshot1 = builder.get_snapshot(
        title="Main View",
        description=f"## Ligand_Fit Results: \n ### {acr} with ligand & electron density map \n - SMILES: {smiles} \n - 2FO-FC at 1.5σ, blue \n - Fitting CC = {cc}",
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
    structure.component(selector="ligand").representation(type="ball_and_stick").color(
        custom={"molstar_color_theme_name": "element-symbol"}
    )
    structure.component(selector="ligand").representation(type="surface").opacity(
        opacity=0.1
    ).color(custom={"molstar_color_theme_name": "element-symbol"})

    structure.component(selector=residue).focus().representation(
        type="ball_and_stick"
    ).color(custom={"molstar_color_theme_name": "element-symbol"})

    ccp4 = builder.download(url=map_file).parse(format="map")
    ccp4.volume().representation(
        type="isosurface",
        relative_isovalue=1.5,
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

    snapshot2 = builder.get_snapshot(
        title="Focus View",
        description=f"## Ligand_Fit Results: \n ### {acr} with ligand & electron density map \n - SMILES: {smiles} \n - 2FO-FC at 1.5σ, blue \n - Fitting CC = {cc}",
        transition_duration_ms=700,
        linger_duration_ms=4000,
    )

    states = mvs.States(
        snapshots=[snapshot1, snapshot2],
        metadata=mvs.GlobalMetadata(description="Ligand_fit Results"),
    )

    with open(pdb_file) as f:
        pdb_data = f.read()

    with open(map_file, mode="rb") as f:
        map_data = f.read()

    html = mvs.molstar_html(
        states,
        data={pdb_file: pdb_data, map_file: map_data},
        ui="stories",
    )

    with open(f"{outdir}/ligand_fit.html", "w") as f:
        f.write(html)
