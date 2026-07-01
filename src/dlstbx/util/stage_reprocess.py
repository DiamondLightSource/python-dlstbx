"""Stage a legacy XChemExplorer ``model_building`` tree for autoprocessing.

Copies the files the PanDDA2 / Pipedream pipeline needs from the **legacy**
XChemExplorer model_building tree into the **autoprocessing** tree the new
pipeline reads from.

    <visit>/processing/analysis/model_building/<dtag>/
        -> <visit>/processing/auto/analysis/model_building/<dtag>/

Only ``complete`` datasets (a single ``compound/<code>.smiles`` with a matching
``<code>.cif``) are copied, so every staged dir is runnable by a ``bulk_array``
job. Incomplete and unstageable datasets are recorded in ``staging_report.json``
but not copied.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import textwrap
from pathlib import Path

# get_pandda_settings() reads autoprocessing.pandda and turns each key/value
# into a --key=value PanDDA2 arg; an empty mapping yields defaults.
_USER_YAML = textwrap.dedent(
    """\
    # Minimal .user.yaml written by the reprocessing staging step.
    autoprocessing:
      pandda: {}
    """
)


def build_stage_plan(ds_dir: Path) -> tuple[dict[Path, str] | None, str]:
    """Build the copy plan for one legacy XChemExplorer dataset dir.

    Core files (``dimple.pdb``, ``dimple.mtz``, ``<dtag>.free.mtz``) are
    required: without them there is nothing for the pipeline to process, so the
    dataset is unstageable -> ``(None, "missing <file>")``. When present, ligand
    files are added and a status classifies how complete the dataset is.
    ``.exists()`` follows symlinks, so a legacy symlink whose target is gone is
    correctly treated as missing.

    ``CompoundCode`` is taken from the ``.smiles`` stem, exactly as the wrappers
    derive it (``pandda_xchem.py``, ``pipedream_xchem.py``).

    Returns ``(plan, status)`` where ``plan`` maps a source ``Path`` to its dest
    path relative to the dataset dir; ``status`` is one of: ``complete``,
    ``no_cif``, ``no_smiles``, ``no_compound``, ``multi_smiles``.
    """
    dtag = ds_dir.name
    dimple_pdb = ds_dir / "dimple.pdb"
    dimple_mtz = ds_dir / "dimple.mtz"
    free_mtz = ds_dir / f"{dtag}.free.mtz"
    for f in (dimple_pdb, dimple_mtz, free_mtz):
        if not f.exists():
            return None, f"missing {f.name}"

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
    overwrite: bool = False,
    logger: logging.Logger | None = None,
) -> dict:
    """Copy ``complete`` legacy datasets from ``src_model`` into ``dst_model``.

    Only datasets with a single ``compound/<code>.smiles`` and a matching
    ``<code>.cif`` are copied, so every staged dir is runnable by a downstream
    ``bulk_array`` job. Copies follow symlinks, so legacy symlinked ``dimple``
    files become real files under ``dst_model``. Datasets already present in
    ``dst_model`` are left untouched unless ``overwrite`` is set.

    Also ensures ``<visit_dir>/.user.yaml`` exists, because the PanDDA2
    wrapper's ``get_pandda_settings()`` opens it unconditionally, and writes a
    ``staging_report.json`` next to ``dst_model`` so incomplete / unstageable
    datasets can be triaged later.

    Raises ``FileNotFoundError`` if ``src_model`` is missing and
    ``PermissionError`` if ``dst_model`` is not writable by the caller.
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

    staged: dict[str, str] = {}  # dtag -> status, present in dst (copied or kept)
    incomplete: dict[str, str] = {}  # dtag -> status, not copied (no usable cif)
    unstageable: dict[str, str] = {}  # dtag -> reason, core files missing
    skipped_existing: list[str] = []  # dtags already present, not overwritten

    for ds in sorted(p for p in src_model.iterdir() if p.is_dir()):
        dtag = ds.name
        plan, status = build_stage_plan(ds)
        if plan is None:
            unstageable[dtag] = status
            continue
        if status != "complete":
            incomplete[dtag] = status
            continue

        dst_dir = dst_model / dtag
        if dst_dir.exists() and not overwrite:
            skipped_existing.append(dtag)
            staged[dtag] = status
            continue

        for src, rel in plan.items():
            dst = dst_dir / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(src, dst)  # follows symlinks -> writes a real file
        staged[dtag] = status

    # The PanDDA2 wrapper opens <visit>/.user.yaml unconditionally.
    user_yaml = visit_dir / ".user.yaml"
    if not user_yaml.exists():
        user_yaml.write_text(_USER_YAML)
        logger.info(f"Wrote minimal {user_yaml}")

    report = {
        "src_model": str(src_model),
        "dst_model": str(dst_model),
        "staged": staged,
        "incomplete": incomplete,
        "unstageable": unstageable,
        "skipped_existing": skipped_existing,
    }
    report_path = dst_model.parent / "staging_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2))

    logger.info(
        f"Staged {len(staged)} complete dataset(s) "
        f"({len(skipped_existing)} already present) from {src_model} to "
        f"{dst_model}; skipped {len(incomplete)} incomplete and "
        f"{len(unstageable)} unstageable. Report: {report_path}"
    )
    return report
