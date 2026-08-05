from __future__ import annotations

import json
from pathlib import Path

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
    analysis_dir,
    database_path,
    *,
    mode="pending_analysis",
    logger=None,
):
    """Write a pipedream_parameters.yaml for manual export_pipedream.py runs.
      Database_path     = database_path (soakDB master, threaded from the trigger)
      Output_directory  = analysis_dir/pipedream/Pipedream_results
      Dataset_csv_path  = Output_directory/datasets.csv

    Processing_directory follows the results base (processing/ or processed/)
    """
    analysis_dir = Path(analysis_dir)
    database_path = Path(database_path)
    # results base: <visit>/processing if processing/auto exists, else <visit>/processed
    base_dir = analysis_dir.parents[1]
    output_dir = analysis_dir / "pipedream" / "Pipedream_results"
    params_path = output_dir / "pipedream_parameters.yaml"
    if params_path.exists():
        if logger:
            logger.info(
                f"Pipedream parameters already exist, leaving as-is: {params_path}"
            )
        return params_path

    text = PIPEDREAM_PARAMETERS_TEMPLATE.format(
        mode=mode,
        processing_dir=base_dir,
        output_dir=output_dir,
        db_path=database_path,
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
    components_to_remove = {"DMS", "EDO", "GOL", "SO4", "PO4", "PEG"}
    removed_counts = dict.fromkeys(components_to_remove, 0)

    kept_lines = []
    for line in lines:
        if line.startswith(("ATOM", "HETATM")):
            resname = line[17:20].strip()
            if resname in components_to_remove:
                removed_counts[resname] += 1
                continue
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


def _pipedream_command(dtag, model_dir, out_dir, ligand_cif):
    """Reconstruct the pipedream command (mirrors pipedream_xchem.py; cosmetic --
    recorded only for provenance, not consumed by collate)."""
    mb = model_dir / dtag
    return (
        "/dls_sw/apps/GPhL/BUSTER/20260424/scripts/pipedream -nolmr "
        f"-hklin {mb / f'{dtag}.free.mtz'} -xyzin {mb / 'dimple.pdb'} "
        f"-hklref {mb / 'dimple.mtz'} -d {out_dir} "
        "-mrefine TLSbasic,WaterUpdatePkmaps -keepwater -remediate "
        "-sidechainrebuild -runpepflip -rhocommands -xclusters -nochirals "
        f"-rhofit {ligand_cif}"
    )


def build_dataset_metadata(dtag, model_dir, pipedream_dir, logger=None):
    """Return the Pipedream_output.json entry for one dtag"""
    out_dir = pipedream_dir / dtag
    if not (out_dir / "pipedream_summary.json").exists():
        return None

    input_dir = model_dir / dtag / "compound"
    smiles_files = sorted(input_dir.glob("*.smiles"))
    if not smiles_files:
        if logger:
            logger.warning(f"{dtag}: no .smiles in {input_dir}, skipping")
        return None
    if len(smiles_files) > 1 and logger:
        logger.warning(
            f"{dtag}: multiple .smiles in {input_dir}, using {smiles_files[0].name}"
        )

    compound_code = smiles_files[0].stem
    smiles = smiles_files[0].read_text().strip()
    ligand_cif = str(input_dir / f"{compound_code}.cif")

    return {
        "Input_dir": str(input_dir),
        "CompoundCode": compound_code,
        "PipedreamDirectory": str(out_dir),
        "ReportHTML": f"{out_dir}/report-{compound_code}/index.html",
        "LigandReportHTML": f"{out_dir}/report-{compound_code}/ligand/index.html",
        "ExpectedSummary": f"{out_dir}/pipedream_summary.json",
        "PipedreamCommand": _pipedream_command(dtag, model_dir, out_dir, ligand_cif),
        "ExpectedCIF": ligand_cif,
        "ExpectedPDB": str(input_dir / f"{compound_code}.pdb"),
        "InputSMILES": smiles,
    }


def build_pipedream_output(analysis_dir, logger=None):
    """Build the {dtag: metadata} aggregate from every completed pipedream
    dataset under a visit's analysis dir.
    """
    analysis_dir = Path(analysis_dir)
    pipedream_dir = analysis_dir / "pipedream"
    model_dir = analysis_dir / "model_building"
    if not pipedream_dir.is_dir():
        raise FileNotFoundError(f"No pipedream directory at {pipedream_dir}")

    data = {}
    for d in sorted(pipedream_dir.iterdir()):
        if not d.is_dir():
            continue
        entry = build_dataset_metadata(d.name, model_dir, pipedream_dir, logger)
        if entry is not None:
            data[d.name] = entry
    return data


def write_pipedream_output(analysis_dir, logger=None):
    """Rebuild pipedream/Pipedream_output.json"""
    analysis_dir = Path(analysis_dir)
    data = build_pipedream_output(analysis_dir, logger)
    target = analysis_dir / "pipedream" / "Pipedream_output.json"

    tmp = target.with_name(target.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    tmp.replace(target)
    if logger:
        logger.info(f"Wrote {len(data)} dataset entries to {target}")
    return target


def cleanup_setvar_files(pipedream_dir, logger=None):
    """Delete __<pid>.setvar.lis autoBUSTER logs left in the pipedream dir.

    BUSTER drops one append-only setvar log per process into its working
    directory (the shared pipedream dir), named after that process's PID. They
    are diagnostic only and never read back, and accumulate over a visit. This
    is called from collate, by which point processing for the visit has
    finished, so any remaining logs are orphaned and safe to remove.
    """
    removed = 0
    for f in Path(pipedream_dir).glob("*.setvar.lis"):
        try:
            f.unlink()
            removed += 1
        except OSError as e:
            if logger:
                logger.warning(f"Could not remove setvar log {f.name}: {e}")

    if removed and logger:
        logger.info(f"Removed {removed} setvar log(s) from {pipedream_dir}")
