from __future__ import annotations

import json
import math
import shutil
from datetime import datetime
from pathlib import Path

import gemmi
import pandas as pd
import yaml

from dlstbx.util.soakdb import update_data_source_bulk
from dlstbx.util.symlink import safe_symlink


def traffic_light(value, green, orange=None, reverse=False):
    """Return the 'green'/'orange'/'red' band for a metric, or None if it
    can't be parsed. Set reverse=True for metrics where higher is better
    (e.g. Ramachandran favoured)."""
    try:
        if value in (None, "", "NA"):
            return None
        val = float(value)
        if orange is None:
            if reverse:
                return "green" if val > green else "red"
            return "green" if val < green else "red"
        if reverse:
            # Higher is better
            if val > green:
                return "green"
            return "orange" if val > orange else "red"
        # Lower is better (R-factor, resolution, RMSD, ...)
        if val < green:
            return "green"
        return "orange" if val < orange else "red"
    except (ValueError, TypeError):
        return None


def find_pipedream_model(pipedream_dir, dtag, rscc_thresh):
    """Locate the Pipedream postrefine model for a dataset and its best rhofit
    RSCC."""

    RHOFIT_HIT_LOG = "Hit_corr.log"

    pipdream_dtag = pipedream_dir / dtag
    if not pipdream_dtag.is_dir():
        return None, None

    rhofit_dir = next(pipdream_dtag.glob("rhofit-*"), None)
    postrefine_dir = next(pipdream_dtag.glob("postrefine-*"), None)
    if not rhofit_dir or not postrefine_dir:
        return None, None

    hit_log = rhofit_dir / RHOFIT_HIT_LOG
    if not hit_log.exists():
        return None, None

    with open(hit_log) as f:
        rscc = max(float(line.split()[1]) for line in f if line.strip())

    refine_pdb = postrefine_dir / "refine.pdb"
    if refine_pdb.exists() and rscc > rscc_thresh:
        return refine_pdb, rscc
    return None, None


def find_pandda_model(panddas_dir, dtag) -> Path | None:
    panddas_dtag = panddas_dir / "processed_datasets" / f"{dtag}"
    pandda_model = panddas_dtag / "modelled_structures" / f"{dtag}-pandda-model.pdb"
    model_path = pandda_model if pandda_model.exists() else None
    return model_path


def pandda_ligand_confidence(panddas_dir, dtag):
    """Map the highest per-event ligand Score (from the dataset's events.yaml)
    to a soakDB RefinementLigandConfidence band. Returns None if there are no
    events to score."""
    events_yaml = panddas_dir / "processed_datasets" / dtag / "events.yaml"
    if not events_yaml.exists():
        return None
    data = yaml.safe_load(events_yaml.read_text()) or {}
    if not data:
        return None
    best = max(event["Score"] for event in data.values())
    if best > 0.9:
        return "4 - High Confidence"
    if best >= 0.7:
        return "2 - Correct ligand, weak density"
    return "1 - Low Confidence"


def pandda_run_fields(panddas_dir, dtag):
    """mainTable PanDDA columns XCE sets for every dataset PanDDA saw, derived
    from the panddas dir rather than the (human) inspect CSV. Empty dict if
    PanDDA didn't process this dataset. DatePanDDAModelCreated is included only
    when a PanDDA model exists, so it is set regardless of which model
    (PanDDA2 or Pipedream) is ultimately selected."""
    processed = panddas_dir / "processed_datasets" / dtag
    if not processed.is_dir():
        return {}
    events_yaml = processed / "events.yaml"
    is_hit = events_yaml.exists() and bool(yaml.safe_load(events_yaml.read_text()))
    fields = {
        "DimplePANDDAwasRun": "True",
        "DimplePANDDApath": str(panddas_dir),
        "DimplePANDDAreject": "False",
        "DimplePANDDAhit": "True" if is_hit else "False",
        "PANDDAStatus": "Finished",
    }
    pandda_model = find_pandda_model(panddas_dir, dtag)
    if pandda_model:
        fields["DatePanDDAModelCreated"] = datetime.fromtimestamp(
            pandda_model.stat().st_mtime
        ).strftime("%Y-%m-%d %H:%M:%S")
    return fields


def dimple_import_fields(dataset_dir, dtag, logger=None):
    """soakDB Dimple/Refinement columns for one dataset, mirroring XCE's
    helpers/update_data_source_for_new_dimple_pdb.py. Empty dict if the dataset
    has no dimple.pdb; Rcryst/Rfree/space group are read from the dimple.pdb
    header via gemmi. RefinementOutcome is set to the 'pending' state XCE uses
    after a DIMPLE run - model selection later overwrites it for chosen hits."""
    dimple_pdb = dataset_dir / "dimple.pdb"
    if not dimple_pdb.is_file():
        return {}

    dimple_mtz = dataset_dir / "dimple.mtz"
    fields = {
        "DimplePathToPDB": str(dimple_pdb),
        "RefinementOutcome": "1 - Analysis Pending",
    }
    if dimple_mtz.is_file():
        fields["DimplePathToMTZ"] = str(dimple_mtz)
        fields["DataProcessingDimpleSuccessful"] = "True"
        fields["DimpleStatus"] = "finished"
    else:
        fields["DataProcessingDimpleSuccessful"] = "False"
        fields["DimpleStatus"] = "failed"

    try:
        st = gemmi.read_structure(str(dimple_pdb))
        if st.spacegroup_hm:
            fields["RefinementSpaceGroup"] = st.spacegroup_hm
        if st.meta.refinement:
            ref = st.meta.refinement[0]
            if ref.r_work is not None and not math.isnan(ref.r_work):
                fields["DimpleRcryst"] = ref.r_work
            if ref.r_free is not None and not math.isnan(ref.r_free):
                fields["DimpleRfree"] = ref.r_free
    except Exception as e:
        if logger:
            logger.warning(f"Could not parse {dimple_pdb} header for {dtag}: {e}")

    free_mtz = dataset_dir / f"{dtag}.free.mtz"
    if free_mtz.is_file():
        # XCE stores the bare filename, resolved relative to the dataset dir
        fields["RefinementMTZfree"] = f"{dtag}.free.mtz"
    return fields


def pipedream_refinement_metrics(
    pipedream_model, compound_code, db_timestamp, logger=None
):
    """Extract refinement & validation statistics from the pipedream_summary.json
    that sits alongside the selected postrefine model, returning a dict of soakDB
    mainTable columns. Mirrors https://github.com/Daren-fearon/pipedream_xchem/.

    Note: RefinementOutcome, RefinementLigandConfidence, RefinementLigandCC and
    RefinementBoundConformation are intentionally left out - they are set from
    the rhofit rscc by the caller."""

    postrefine_dir = Path(pipedream_model).parent
    pipedream_out = postrefine_dir.parent
    summary_path = pipedream_out / "pipedream_summary.json"

    with open(summary_path) as f:
        summary = json.load(f)

    ligands = summary.get("ligandfitting", {}).get("ligands", [])
    first_ligand = ligands[0] if ligands else {}
    molprobity = first_ligand.get("validationstatistics", {}).get("molprobity", {})
    # postrefinement[1] is the final refinement step (matches collate script)
    postref = first_ligand.get("postrefinement", [])
    postref_final = postref[1] if len(postref) > 1 else {}

    # High resolution from data processing input, rounded for display
    reshigh = summary.get("dataprocessing", {}).get("inputdata", {}).get("reshigh")
    try:
        resolution = round(float(reshigh), 2)
    except (TypeError, ValueError):
        resolution = None

    def _round(value, digits=3):
        return round(value, digits) if isinstance(value, (int, float)) else value

    r = _round(postref_final.get("R"))
    rfree = _round(postref_final.get("Rfree"))
    molprob = molprobity.get("molprobityscore")
    rama_out = molprobity.get("ramaoutlierpercent")
    rama_fav = molprobity.get("ramafavoredpercent")
    rmsd_bonds = molprobity.get("rmsbonds")
    rmsd_angles = molprobity.get("rmsangles")

    # BUSTER mmCIF model/reflections and the report HTML live in the
    # postrefine / report directories of the same Pipedream output
    mmcif_model = postrefine_dir / "BUSTER_model.cif"
    mmcif_reflections = postrefine_dir / "BUSTER_refln.cif"
    report = pipedream_out / f"report-{compound_code}" / "index.html"

    if logger and not mmcif_model.exists():
        logger.warning(f"BUSTER model CIF not found at {mmcif_model}")
    if logger and not mmcif_reflections.exists():
        logger.warning(f"BUSTER reflections CIF not found at {mmcif_reflections}")

    return {
        "RefinementResolution": resolution,
        "RefinementResolutionTL": traffic_light(resolution, 2.0, 2.5),
        "RefinementRcryst": r,
        "RefinementRcrystTraficLight": traffic_light(r, 0.20, 0.25),
        "RefinementRfree": rfree,
        "RefinementRfreeTraficLight": traffic_light(rfree, 0.25, 0.30),
        "RefinementOutcomePerson": "gda2",
        "RefinementOutcomeDate": db_timestamp,
        "RefinementPDB_latest": str(pipedream_model),
        "RefinementMTZ_latest": str(postrefine_dir / "refine.mtz"),
        "RefinementMMCIFmodel_latest": str(mmcif_model),
        "RefinementMMCIFreflections_latest": str(mmcif_reflections),
        "RefinementMolProbityScore": molprob,
        "RefinementMolProbityScoreTL": traffic_light(molprob, 2, 3),
        "RefinementRamachandranOutliers": rama_out,
        "RefinementRamachandranOutliersTL": traffic_light(rama_out, 0.3, 1),
        "RefinementRamachandranFavored": rama_fav,
        "RefinementRamachandranFavoredTL": traffic_light(
            rama_fav, 98, 95, reverse=True
        ),
        "RefinementRmsdBonds": rmsd_bonds,
        "RefinementRmsdBondsTL": traffic_light(rmsd_bonds, 0.012, 0.018),
        "RefinementRmsdAngles": rmsd_angles,
        "RefinementRmsdAnglesTL": traffic_light(rmsd_angles, 1.5, 2.0),
        "RefinementStatus": "finished",
        "RefinementBusterReportHTML": str(report),
        "RefinementDate": db_timestamp,
    }


def export_pipedream_files(
    dataset_dir, compound_code, pipedream_dir, dtag, logger=None
):
    """Export Pipedream results to model_building directory"""

    compound_dir = dataset_dir / "compound"  # in model_building
    target_cif = compound_dir / f"{compound_code}.cif"
    target_pdb = compound_dir / f"{compound_code}.pdb"
    symlink_cif = dataset_dir / f"{compound_code}.cif"

    rhofit_dir = pipedream_dir / dtag / f"rhofit-{compound_code}"
    output_cif_file = rhofit_dir / "best.cif"
    refined_pdb_file = rhofit_dir / "best.pdb"

    if refined_pdb_file.exists() and output_cif_file.exists():
        shutil.copy2(refined_pdb_file, target_pdb)
        shutil.copy2(output_cif_file, target_cif)
        safe_symlink(target_cif, symlink_cif, logger)
    elif logger:
        logger.info(f"Could not export restraints files for {dataset_dir}")

    mtz_file_dest = pipedream_dir / dtag / f"postrefine-{compound_code}" / "refine.mtz"
    postrefine_pdb = pipedream_dir / dtag / f"postrefine-{compound_code}" / "refine.pdb"

    safe_symlink(postrefine_pdb, dataset_dir / "refine.pdb", logger)
    safe_symlink(mtz_file_dest, dataset_dir / "refine.mtz", logger)
    safe_symlink(postrefine_pdb, dataset_dir / "refine.split.bound-state.pdb", logger)

    safe_symlink(
        pipedream_dir / dtag / f"postrefine-{compound_code}" / "refine_2fofc.map",
        dataset_dir / f"{dtag}_2fofc.map",
        logger,
    )
    safe_symlink(
        pipedream_dir / dtag / f"postrefine-{compound_code}" / "refine_fofc.map",
        dataset_dir / f"{dtag}_fofc.map",
        logger,
    )


def symlink_score_buckets(panddas_dir, pandda_dir, updatable, logger):
    """Build score-bucketed, symlinked copies of the PanDDA processed_datasets
    dir so models can be browsed by ligand score. Only datasets still in
    `updatable` (not yet processed by the autopipeline) are bucketed.

    Scores come from the per-dataset best_score.txt written by pandda_xchem."""

    processed_dataset_dir = panddas_dir / "processed_datasets"
    events_csv = panddas_dir / "analyses" / "pandda_analyse_events.csv"
    sites_csv = panddas_dir / "analyses" / "pandda_analyse_sites.csv"

    if not events_csv.exists():
        logger.info(f"No {events_csv}, skipping score bucketing")
        return

    # scores[dtag] = best ligand score
    scores = {}
    for dataset in processed_dataset_dir.iterdir():
        if dataset.name not in updatable:
            continue
        score_file = dataset / "best_score.txt"
        if score_file.exists():
            scores[dataset.name] = float(score_file.read_text().strip())

    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    df = pd.read_csv(events_csv, index_col=0)
    buckets = [0.6, 0.8, 0.9, 1]  # boundaries
    for j in range(len(buckets) - 1):
        bucket_dtags = [
            dtag
            for dtag, score in sorted_scores
            if buckets[j] < score <= buckets[j + 1]
        ]
        logger.info(
            f"Datasets scored {buckets[j]}-{buckets[j + 1]}: {len(bucket_dtags)}"
        )
        if not bucket_dtags:
            continue

        # Mirror the panddas layout: analyses/ and processed_datasets/ siblings
        bucket_root = pandda_dir / f"score_{buckets[j]}-{buckets[j + 1]}"
        bucket_processed = bucket_root / "processed_datasets"
        analyses_dir = bucket_root / "analyses"
        bucket_processed.mkdir(parents=True, exist_ok=True)
        analyses_dir.mkdir(parents=True, exist_ok=True)

        # Add this batch's datasets alongside any symlinked by previous runs
        for dtag in bucket_dtags:
            safe_symlink(processed_dataset_dir / dtag, bucket_processed / dtag, logger)

        # Filter the events csv on every dataset now in the bucket (this batch
        # plus earlier ones) so prior batches stay represented.
        all_bucket_dtags = [p.name for p in bucket_processed.iterdir()]
        shutil.copy(sites_csv, analyses_dir)
        filtered = df[df["dtag"].isin(all_bucket_dtags)].reset_index(drop=True)
        filtered.to_csv(analyses_dir / "pandda_analyse_events.csv")


def update_xchem_database(
    model_dir, pipedream_dir, panddas_dir, db_copy, updatable, logger
):
    """Performs model selection & exports results to XChem SoakDB database.
    `updatable` is the CrystalName set this run is allowed to write, captured
    before any RefinementOutcome is set."""

    # Build list of dicts for batch updating rows in SQLite
    db_dicts = []
    for dataset_dir in model_dir.iterdir():
        if not dataset_dir.is_dir():
            continue
        dtag = dataset_dir.name
        if dtag not in updatable:
            logger.info(f"{dtag} not in set of updatable CrystalNames")
            continue
        compound_dir = dataset_dir / "compound"
        cif_files = list(compound_dir.glob("*.cif"))

        if not cif_files:
            logger.info(f"No .cif file in {compound_dir}, skipping {dtag}")
            continue
        if len(cif_files) > 1:
            logger.error(f"Multiple .cif files in {compound_dir}")

        CompoundCode = cif_files[0].stem
        db_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f")[:-4]

        pipedream_model, rscc = find_pipedream_model(
            pipedream_dir, dtag, rscc_thresh=0.7
        )
        pandda_model = find_pandda_model(panddas_dir, dtag)

        # DimplePANDDA* flags XCE sets for every dataset PanDDA ran on, merged
        # into whichever model branch is selected below.
        pandda_fields = pandda_run_fields(panddas_dir, dtag)

        # Export
        if pipedream_model:
            logger.info(f"Selected Pipedream model for {dtag}")

            # Determine ligand confidence based on overall ligandcc value
            if rscc >= 0.8:
                RefinementLigandConfidence = "4 - High Confidence"
                RefinementOutcome = "4 - CompChem ready"
            elif rscc >= 0.7:
                RefinementLigandConfidence = "2 - Correct ligand, weak density"
                RefinementOutcome = "3 - In Refinement"

            # Full refinement/validation statistics from the Pipedream summary json
            try:
                metrics = pipedream_refinement_metrics(
                    pipedream_model, CompoundCode, db_timestamp, logger
                )
            except Exception as e:
                logger.error(f"Could not read Pipedream summary for {dtag}: {e}")
                metrics = {}

            db_dicts.append(
                {
                    "CrystalName": dtag,
                    "RefinementBoundConformation": str(pipedream_model),
                    "RefinementOutcome": RefinementOutcome,
                    "RefinementLigandConfidence": RefinementLigandConfidence,
                    "RefinementLigandCC": rscc,
                    "RefinementCIF": str(
                        dataset_dir / "compound" / f"{CompoundCode}.cif"
                    ),
                    "RefinementCIFprogram": "Grade2",
                    "LastUpdated": db_timestamp,
                    "LastUpdated_by": "gda2",
                    **pandda_fields,
                    **metrics,
                }
            )

            export_pipedream_files(
                dataset_dir, CompoundCode, pipedream_dir, dtag, logger
            )

        elif pandda_model:
            logger.info(f"Selected PanDDA2 model for {dtag}")
            # DatePanDDAModelCreated comes from pandda_fields (set whenever a
            # PanDDA model exists), so it isn't repeated here.
            db_dicts.append(
                {
                    "CrystalName": dtag,
                    "RefinementBoundConformation": str(pandda_model),
                    "RefinementOutcome": "2 - PANDDA model",
                    "RefinementCIFprogram": "Grade2",
                    "RefinementLigandConfidence": pandda_ligand_confidence(
                        panddas_dir, dtag
                    ),
                    "LastUpdated": db_timestamp,
                    "LastUpdated_by": "gda2",
                    **pandda_fields,
                }
            )
        else:
            logger.info(f"No model selected for {dtag}")
            db_dicts.append(
                {
                    "CrystalName": dtag,
                    "RefinementOutcome": "7 - Analysed & Rejected",
                    "LastUpdated": db_timestamp,
                    "LastUpdated_by": "gda2",
                    **pandda_fields,
                }
            )

    # Now update the database with the formed dicts
    try:
        update_data_source_bulk(db_dicts, db_copy)
        logger.debug(f"Bulk updated {db_copy} for {len(db_dicts)} datasets")
    except Exception as e:
        logger.debug(f"Could not bulk update {db_copy}: {e}")


def update_dimple_columns(model_dir, db_copy, updatable, logger):
    """Import DIMPLE results into the soakDB for every dataset with a dimple.pdb,
    mirroring the per-dataset dimple import XCE runs after DIMPLE. Gated by
    `updatable` so curated rows are left alone. Covers apo datasets (no compound
    cif) that model selection skips.

    Run before update_xchem_database: this sets RefinementOutcome to
    '1 - Analysis Pending', and model selection then overwrites it with the
    final outcome for datasets where a ligand model is chosen."""
    db_dicts = []
    for dataset_dir in model_dir.iterdir():
        if not dataset_dir.is_dir():
            continue
        dtag = dataset_dir.name
        if dtag not in updatable:
            continue
        fields = dimple_import_fields(dataset_dir, dtag, logger)
        if not fields:
            continue
        fields["CrystalName"] = dtag
        fields["LastUpdated"] = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f")[:-4]
        fields["LastUpdated_by"] = "gda2"
        db_dicts.append(fields)

    if not db_dicts:
        return
    try:
        update_data_source_bulk(db_dicts, db_copy)
        logger.debug(f"Imported dimple columns for {len(db_dicts)} datasets")
    except Exception as e:
        logger.debug(f"Could not bulk update dimple columns in {db_copy}: {e}")
