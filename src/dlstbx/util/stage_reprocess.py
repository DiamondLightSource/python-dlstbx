"""Stage a legacy XChem ``model_building`` tree for autoprocessing.

Copies the files the PanDDA2 / Pipedream pipeline needs from an existing
XChem model_building tree into the autoprocessing tree.

    <visit>/processing/analysis/model_building/<dtag>/
        -> <visit>/processing/auto/analysis/model_building/<dtag>/

Ligand datasets are copied only when ``complete`` (a single
``compound/<code>.smiles`` with a matching ``<code>.cif``), so every staged dir
holding a ``.smiles`` is runnable by a ``bulk_array`` job. Apo / ground-state
datasets are copied without ligand files, mirroring ``trigger_xchem`` behaviour
for live apo collections. DMSO/solvent soaks are excluded. Everything not copied
is recorded in output ``staging_report.json``.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
from pathlib import Path

from dlstbx.util.soakdb import soakdb_path

# Apo / ground-state crystals carry no ligand. They are staged with core files
# only, because PanDDA2 uses them as ground-state comparators.
APO_STATUSES = frozenset({"no_compound", "no_smiles"})

# `no_cif` and `multi_smiles` are deliberately left unstaged.
STAGED_STATUSES = APO_STATUSES | {"complete"}

# DMSO / solvent soaks are screens, not real ligand experiments.
_DMSO_SMILES = "CS(=O)C"
_SOLVENT_LIBRARIES = frozenset({"DMSO", "SOLVENT"})


def dimple_source(ds_dir: Path, ext: str) -> Path | None:
    """Locate a legacy dimple output, preferring the top-level symlink."""
    top = ds_dir / f"dimple.{ext}"
    if top.exists():
        return top
    final = ds_dir / "dimple" / "dimple" / f"final.{ext}"
    return final if final.exists() else None


def solvent_soak_dtags(db_path: Path, logger: logging.Logger) -> set[str]:
    """Return the dtags soakDB marks as DMSO/solvent soaks."""
    if not db_path.is_file():
        logger.warning(f"No soakDB at {db_path}; continuing without a solvent filter")
        return set()

    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
    try:
        rows = con.execute(
            "SELECT CrystalName, LibraryName, CompoundSMILES FROM mainTable "
            "WHERE CrystalName IS NOT NULL ORDER BY ID"
        ).fetchall()
    finally:
        con.close()

    latest = {dtag: (library, smiles) for dtag, library, smiles in rows}
    return {
        dtag
        for dtag, (library, smiles) in latest.items()
        if (library or "").strip().upper() in _SOLVENT_LIBRARIES
        or (smiles or "").strip() == _DMSO_SMILES
    }


def build_stage_plan(ds_dir: Path) -> tuple[dict[Path, str] | None, str]:
    """Build the copy plan for one legacy XChemExplorer dataset dir.

    Core files (dimple pdb + mtz, ``<dtag>.free.mtz``) are required: without
    them there is nothing for the pipeline to process, so the dataset is
    unstageable -> ``(None, "missing <file>")``. When present, ligand files are
    added and a status classifies how complete the dataset is.

    Returns ``(plan, status)`` where ``plan`` maps a source ``Path`` to its dest
    path relative to the dataset dir; ``status`` is one of: ``complete``,
    ``no_cif``, ``no_smiles``, ``no_compound``, ``multi_smiles``. The dimple
    files land under their canonical names whichever source they came from.
    """
    dtag = ds_dir.name
    dimple_pdb = dimple_source(ds_dir, "pdb")
    if dimple_pdb is None:
        return None, "missing dimple.pdb"
    dimple_mtz = dimple_source(ds_dir, "mtz")
    if dimple_mtz is None:
        return None, "missing dimple.mtz"
    free_mtz = ds_dir / f"{dtag}.free.mtz"
    if not free_mtz.exists():
        return None, f"missing {free_mtz.name}"

    plan = {
        dimple_pdb: "dimple.pdb",
        dimple_mtz: "dimple.mtz",
        free_mtz: f"{dtag}.free.mtz",
    }

    compound = ds_dir / "compound"
    if not compound.is_dir():
        return plan, "no_compound"

    smiles_files = sorted(compound.glob("*.smiles"))
    if len(smiles_files) == 0:
        return plan, "no_smiles"
    if len(smiles_files) > 1:
        return plan, "multi_smiles"  # ambiguous: don't copy ligand files
    smiles = smiles_files[0]
    cc = smiles.stem  # CompoundCode, as the wrappers derive it
    plan[smiles] = f"compound/{smiles.name}"

    cc_pdb = compound / f"{cc}.pdb"
    if cc_pdb.exists():
        plan[cc_pdb] = f"compound/{cc_pdb.name}"

    cif = compound / f"{cc}.cif"
    if cif.exists():
        plan[cif] = f"compound/{cif.name}"
        return plan, "complete"
    return plan, "no_cif"


def stage_legacy_model_building(
    src_model: Path,
    dst_model: Path,
    *,
    visit_dir: Path,
    logger: logging.Logger | None = None,
) -> dict:
    """Copy the runnable legacy datasets from ``src_model`` into ``dst_model``.

    Ligand datasets are copied only when they hold a single
    ``compound/<code>.smiles`` with a matching ``<code>.cif``.

    Apo datasets are copied without ligand files, and DMSO/solvent soaks are
    skipped entirely — both matching what ``trigger_xchem`` does live. Copies
    follow symlinks, so legacy symlinked ``dimple`` files become real files
    under ``dst_model``. Datasets already staged into ``dst_model`` are left
    alone, so a re-run only fills in what is missing.

    Writes a ``staging_report.json`` next to ``dst_model`` so incomplete /
    unstageable datasets can be triaged later.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if not src_model.is_dir():
        raise FileNotFoundError(f"source model_building not found: {src_model}")

    # Writability guard: walk up to the first existing ancestor of dst_model.
    parent = dst_model
    while not parent.exists():
        parent = parent.parent
    if not os.access(parent, os.W_OK):
        raise PermissionError(
            f"No write access to {parent}; run the trigger as a member of the "
            "visit group (e.g. gda2)."
        )

    solvent_soaks = solvent_soak_dtags(soakdb_path(visit_dir), logger)

    staged: dict[str, str] = {}  # dtag -> status, present in dst (copied or kept)
    incomplete: dict[str, str] = {}  # dtag -> status, not copied (no usable cif)
    unstageable: dict[str, str] = {}  # dtag -> reason, core files missing
    dmso_excluded: list[str] = []  # dtags dropped as DMSO/solvent soaks
    skipped_existing: list[str] = []  # dtags already present, not overwritten

    for ds in sorted(p for p in src_model.iterdir() if p.is_dir()):
        dtag = ds.name
        if dtag in solvent_soaks:
            dmso_excluded.append(dtag)
            continue
        plan, status = build_stage_plan(ds)
        if plan is None:
            unstageable[dtag] = status
            continue
        if status not in STAGED_STATUSES:
            incomplete[dtag] = status
            continue

        dst_dir = dst_model / dtag
        if dst_dir.exists():
            skipped_existing.append(dtag)
            staged[dtag] = status
            continue

        for src, rel in plan.items():
            dst = dst_dir / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(src, dst)  # follows symlinks -> writes a real file
        staged[dtag] = status

    report = {
        "src_model": str(src_model),
        "dst_model": str(dst_model),
        "staged": staged,
        "incomplete": incomplete,
        "unstageable": unstageable,
        "dmso_excluded": dmso_excluded,
        "skipped_existing": skipped_existing,
    }
    report_path = dst_model.parent / "staging_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2))

    n_apo = sum(1 for status in staged.values() if status in APO_STATUSES)
    logger.info(
        f"Staged {len(staged)} dataset(s), {n_apo} of them apo "
        f"({len(skipped_existing)} already present) from {src_model} to "
        f"{dst_model}; skipped {len(incomplete)} incomplete, "
        f"{len(unstageable)} unstageable and {len(dmso_excluded)} DMSO/solvent. "
        f"Report: {report_path}"
    )
    return report
