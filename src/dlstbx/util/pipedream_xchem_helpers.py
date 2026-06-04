from __future__ import annotations

import json
import os
from pathlib import Path

import portalocker

# Fixed body of pipedream_parameters.yaml. Only the five path/mode keys at the
# top vary between runs; everything from Cluster_partition down is constant.
PIPEDREAM_PARAMETERS_TEMPLATE = """\
Mode: "{mode}" # 'pending_analysis' or 'specific_datasets' - the former will parse your database file for all datasets with RefinementOutcome "1 - Pending Analysis", the later will use a specified list of datasets provided in the csv file specified below

Processing_directory: {processing_dir}
Output_directory: {output_dir} # Optional - defaults to Processing_directory/analysis/Pipedream/Pipedream_<timestamp> if not set
Database_path: {db_path}
Dataset_csv_path: {csv_path} # Only required if Mode is 'specific_datasets'

# Cluster Configuration (Optional)
Cluster_partition: "cs05r"  # Options: cs05r, cs04r (default: cs05r)
Job_priority: "normal"          # Options: normal, low, high (default: normal)
                              # low = nice 1000 (runs after other jobs), high = nice -100

Remove_crystallisation_components: true  # Optional - removes DMS, EDO, GOL, SO4, PO4, PEG from input PDBs if true (can skip if not modelled in site of interest in MR model)
Refinement_parameters: #For more information see https://www.globalphasing.com/buster/manual/pipedream/manual/index.html#_details_of_command_line_arguments
  keepwater: true #DO NOT remove waters that are present in the input model (default is to remove them)
  WaterUpdatePkmaps: true #Update water pkmaps during refinement
  TLS: "TLSbasic" #"TLSbasic" turns on TLS refinement and autoncs. Leave blank for no TLS.
  remediate: true #Run SideAide to refit side chains
  sidechainrebuild: true #Allow SideAide to rebuild stubbed sidechains
  runpepflip: true #Run pepflip to check for and correct peptide bond flips
  rhocommands:
    - -xclusters # Produces ligand fits for the <n> best possible binding sites. Leave blank for default and fit to <NCS> best sites.
    - -nochirals # Ignore CHIRAL restraints in fitting/output. Chiral centres can then invert as needed.
"""


def write_pipedream_parameters(
    processing_dir,
    pipedream_dir,
    *,
    mode="specific_datasets",
    logger=None,
):
    """Write a pipedream_parameters.yaml for manual export_pipedream.py runs.

    The refinement/cluster block is fixed; only the paths change, and they all
    derive from the two directories the wrapper already computes:

      Database_path     = processing_dir/database/soakDBDataFile.sqlite (master)
      Output_directory  = pipedream_dir/Pipedream_results
      Dataset_csv_path  = Output_directory/datasets.csv

    The file is written into Output_directory and its path returned.
    """
    processing_dir = Path(processing_dir)
    output_dir = Path(pipedream_dir) / "Pipedream_results"
    params_path = output_dir / "pipedream_parameters.yaml"
    if params_path.exists():
        if logger:
            logger.info(
                f"Pipedream parameters already exist, leaving as-is: {params_path}"
            )
        return params_path

    text = PIPEDREAM_PARAMETERS_TEMPLATE.format(
        mode=mode,
        processing_dir=processing_dir,
        output_dir=output_dir,
        db_path=processing_dir / "database" / "soakDBDataFile.sqlite",
        csv_path=output_dir / "datasets.csv",
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    params_path.write_text(text, encoding="utf-8")
    if logger:
        logger.info(f"Wrote pipedream parameters to {params_path}")
    return params_path


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
