from __future__ import annotations

import json
import os
from pathlib import Path

import portalocker


def process_pdb_file(dimple_pdb: Path, logger=None):
    """Strip common crystallisation components from a dimple pdb in-place."""
    if not dimple_pdb.exists():
        if logger:
            logger.debug(f"Dimple pdb {dimple_pdb} does not exist")
        return True

    with open(dimple_pdb, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Count removals by component type
    original_count = len(lines)
    components_to_remove = ["DMS", "EDO", "GOL", "SO4", "PO4", "PEG"]
    removed_counts = dict.fromkeys(components_to_remove, 0)

    kept_lines = []
    for line in lines:
        if any(res in line for res in components_to_remove):
            # Count which component was found
            for comp in components_to_remove:
                if comp in line:
                    removed_counts[comp] += 1
                    break
        else:
            kept_lines.append(line)

    # Write cleaned file
    with open(dimple_pdb, "w", encoding="utf-8") as f:
        f.writelines(kept_lines)

    removed_total = original_count - len(kept_lines)
    if removed_total > 0 and logger:
        component_summary = ", ".join(
            [f"{comp}: {count}" for comp, count in removed_counts.items() if count > 0]
        )
        logger.debug(f"Removed {removed_total} lines. ({component_summary})")


def save_dataset_metadata(
    pipedream_dir,
    input_dir,
    output_dir,
    compound_code,
    smiles_string,
    pipedream_cmd,
    dtag,
    logger=None,
):
    metadata = {
        "Input_dir": input_dir,
        "CompoundCode": compound_code,
        "PipedreamDirectory": output_dir,
        "ReportHTML": f"{output_dir}/report-{compound_code}/index.html",
        "LigandReportHTML": f"{output_dir}/report-{compound_code}/ligand/index.html",
        "ExpectedSummary": f"{output_dir}/pipedream_summary.json",
        "PipedreamCommand": pipedream_cmd,
        "ExpectedCIF": os.path.join(input_dir, f"{compound_code}.cif"),
        "ExpectedPDB": os.path.join(input_dir, f"{compound_code}.pdb"),
        "InputSMILES": smiles_string,
    }

    output_yaml = {}
    output_yaml[dtag] = metadata
    json_file = f"{pipedream_dir}/Pipedream_output.json"
    if not os.path.exists(json_file):
        open(json_file, "w").close()

    # Acquire a lock
    with portalocker.Lock(json_file, timeout=5):
        if os.path.exists(json_file) and os.path.getsize(json_file) > 0:
            with open(json_file, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except Exception as e:
                    if logger:
                        logger.debug(
                            f"Cannot continue with pipedream postprocessing: {e}"
                        )
                    return
        else:
            data = {}

        data.update(output_yaml)

        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
