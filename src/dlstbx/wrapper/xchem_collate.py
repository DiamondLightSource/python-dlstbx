from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
from datetime import datetime
from itertools import groupby
from pathlib import Path

from dlstbx.wrapper import Wrapper


class XChemCollateWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.xchem_collate"

    def run(self):
        """Performs collation of PanDDA2 & Pipedream results for a labxchem visit.
        Runs automated model selection and re-integrates results back into soakDB,
        and XChem evironment."""

        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.info(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        params = self.recwrap.recipe_step["job_parameters"]
        pipedream = params.get("pipedream")
        overwrite = params.get("overwrite")
        processing_dir = Path(params.get("processing_directory"))
        auto_dir = processing_dir / "auto"
        analysis_dir = auto_dir / "analysis"
        pandda_dir = analysis_dir / "pandda2"
        model_dir = analysis_dir / "model_building"
        panddas_dir = Path(pandda_dir / "panddas")
        pipedream_dir = analysis_dir / "pipedream"

        # -------------------------------------------------------
        # Collate PanDDA2 results --> events & sites csv

        if panddas_dir.exists():
            pandda2_command = f"source /dls_sw/i04-1/software/PanDDA2/venv/bin/activate; \
            python -u /dls_sw/i04-1/software/PanDDA2/scripts/postrun.py --data_dirs={model_dir} --out_dir={panddas_dir} --use_ligand_data=False --debug=True --local_cpus=4 > {panddas_dir / 'pandda2_postrun.log'}"

            self.log.info(f"Running XChemCollate command: {pandda2_command}")

            try:
                subprocess.run(
                    pandda2_command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    cwd=panddas_dir,
                    check=True,
                    timeout=params.get("timeout-minutes") * 60,
                )

            except subprocess.CalledProcessError as e:
                self.log.error(f"XChemCollate command: '{pandda2_command}' failed")
                self.log.info(e.stdout)
                self.log.error(e.stderr)

        # -------------------------------------------------------
        # Perform model selection (PanDDA2/Pipedream) & re-integrate into XChem environment

        try:
            self.update_xchem_database(
                processing_dir, model_dir, pipedream_dir, panddas_dir, overwrite
            )
        except Exception as e:
            self.log.error(f"Exception updating database for {processing_dir}: {e}")

        # -------------------------------------------------------
        # Perform Pipedream collate --> html output
        if pipedream:
            pipedream_command = f"module load mamba;  mamba activate /dls/science/groups/i04-1/software/micromamba/envs/xchem; \
            python /dls/science/groups/i04-1/software/pipedream_xchem/collate_pipedream_results.py --input {pipedream_dir / 'Pipedream_output.json'} --no-browser --no-plots -v"

            self.log.info(f"Running XChemCollate command: {pipedream_command}")

            try:
                subprocess.run(
                    pipedream_command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    cwd=panddas_dir,
                    check=True,
                    timeout=params.get("timeout-minutes") * 60,
                )

            except subprocess.CalledProcessError as e:
                self.log.error(f"XChemCollate command: '{pipedream_command}' failed")
                self.log.info(e.stdout)
                self.log.error(e.stderr)
        else:
            self.log.info(
                f"Skipping collation of Pipedream results for {pipedream_dir}"
            )

        # -------------------------------------------------------
        # Perform XChemAlign collate step
        xca_dir = analysis_dir / "xchemalign"
        config = xca_dir / "config.yaml"
        assemblies = xca_dir / "assemblies.yaml"

        if not config.exists() or not assemblies.exists():
            self.log.info(
                f"No config/assemblies .yaml in {str(xca_dir)}, skipping autoXCA"
            )
        else:
            autoxca_dir = auto_dir / "xchemalign"  # do relative paths work from here?
            shutil.copy(config, autoxca_dir / "config.yaml")
            shutil.copy(assemblies, autoxca_dir / "assemblies.yaml")

            xca_command = f"source /dls/science/groups/i04-1/software/xchem-align/act; conda activate /dls/science/groups/i04-1/software/xchem-align/env_xchem_align; \
            python -m xchemalign.collator -d {xca_dir}; python -m xchemalign.aligner -d {xca_dir}"

            self.log.info("Running XCA command: {xca_command}")

            try:
                subprocess.run(
                    xca_command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    cwd=autoxca_dir,
                    check=True,
                    timeout=params.get("timeout-minutes") * 60,
                )

            except subprocess.CalledProcessError as e:
                self.log.error(f"XCA command: '{xca_command}' failed")
                self.log.info(e.stdout)
                self.log.error(e.stderr)

        self.log.info("Auto XChemCollate finished successfully")
        return True

    # -------------------------------------------------------

    def find_pipedream_model(self, pipedream_dir, dtag, rscc_thresh):
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

    def find_pandda_model(self, panddas_dir, dtag) -> Path | None:
        panddas_dtag = panddas_dir / "processed_datasets" / f"{dtag}"
        pandda_model = panddas_dtag / "modelled_structures" / f"{dtag}-pandda-model.pdb"
        model_path = pandda_model if pandda_model.exists() else None
        return model_path

    def update_xchem_database(
        self, processing_dir, model_dir, pipedream_dir, panddas_dir, overwrite=False
    ):
        """Performs model selection & exports results to XChem SoakDB database"""

        db_master = processing_dir / "database" / "soakDBDataFile.sqlite"
        db_copy = processing_dir / "auto/database" / "autosoakDBDataFile.sqlite"

        if not db_copy.exists():
            Path(db_copy.parents[0]).mkdir(parents=True, exist_ok=True)
            shutil.copy(db_master, db_copy)

        self.sync_schema_from_master(db_master, db_copy, "mainTable")
        self.sync_rows_from_master(db_master, db_copy, "mainTable")

        # The CrystalName rows the db update is allowed to edit
        updatable = self.updatable_crystals(db_copy, overwrite)

        # Build list of dicts for batch updating rows in SQLite
        db_dicts = []
        for dataset_dir in model_dir.iterdir():
            dtag = dataset_dir.name
            if dtag not in updatable:
                continue
            compound_dir = dataset_dir / "compound"
            cif_files = list(compound_dir.glob("*.cif"))

            if len(cif_files) > 1:
                self.log.error(f"Multiple .cif files in {compound_dir}")

            CompoundCode = cif_files[0].stem
            db_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f")[:-4]

            pipedream_model, rscc = self.find_pipedream_model(
                pipedream_dir, dtag, rscc_thresh=0.7
            )
            pandda_model = self.find_pandda_model(panddas_dir, dtag)

            # Export
            if pipedream_model:
                self.log.info(f"Selected Pipedream model for {dtag}")

                # Determine ligand confidence based on overall ligandcc value
                if rscc >= 0.8:
                    RefinementLigandConfidence = "4 - High Confidence"
                    RefinementOutome = "4 - CompChem ready"
                elif rscc >= 0.7:
                    RefinementLigandConfidence = "2 - Correct ligand, weak density"
                    RefinementOutome = "3 - In Refinement"

                # Full refinement/validation statistics from the Pipedream summary json
                try:
                    metrics = self.pipedream_refinement_metrics(
                        pipedream_model, CompoundCode, db_timestamp
                    )
                except Exception as e:
                    self.log.error(f"Could not read Pipedream summary for {dtag}: {e}")
                    metrics = {}

                db_dicts.append(
                    {
                        "CrystalName": dtag,
                        "RefinementBoundConformation": str(pipedream_model),
                        "RefinementOutcome": RefinementOutome,
                        "RefinementLigandConfidence": RefinementLigandConfidence,
                        "RefinementLigandCC": rscc,
                        "RefinementCIF": str(
                            dataset_dir / "compound" / f"{CompoundCode}.cif"
                        ),
                        "RefinementCIFprogram": "Grade2",
                        "LastUpdated": db_timestamp,
                        "LastUpdated_by": "gda2",
                        **metrics,
                    }
                )

                self.export_pipedream_files(
                    dataset_dir, CompoundCode, pipedream_dir, dtag
                )

            elif pandda_model:
                self.log.info(f"Selected PanDDA2 model for {dtag}")
                db_dicts.append(
                    {
                        "CrystalName": dtag,
                        "RefinementBoundConformation": str(pandda_model),
                        "RefinementOutcome": "2 - PANDDA model",
                        "RefinementCIFprogram": "Grade2",
                        "LastUpdated": db_timestamp,
                        "LastUpdated_by": "gda2",
                    }
                )
            else:
                self.log.info(f"No model selected for {dtag}")
                db_dicts.append(
                    {
                        "CrystalName": dtag,
                        "RefinementOutcome": "7 - Analysed & Rejected",
                        "LastUpdated": db_timestamp,
                        "LastUpdated_by": "gda2",
                    }
                )

        # Now update the database with the formed dicts
        try:
            self.update_data_source_bulk(db_dicts, db_copy)
            self.log.debug(f"Bulk updated {db_copy} for {len(db_dicts)} datasets")
        except Exception as e:
            self.log.debug(f"Could not bulk update {db_copy}: {e}")

    def updatable_crystals(self, database_path, overwrite=False):
        """CrystalNames this run is allowed to write — both the skip-set for
        building db_dicts/exporting files and the gate for the bulk update.

        default: rows not yet given a RefinementOutcome.
        overwrite: also rows whose RefinementOutcome was set by a previous
        automated run (LastUpdated_by 'gda2', or never touched), while leaving
        manually-curated rows (any other LastUpdated_by) alone."""
        if overwrite:
            where = "(LastUpdated_by = 'gda2' OR LastUpdated_by IS NULL)"
        else:
            where = "RefinementOutcome IS NULL"
        conn = sqlite3.connect(database_path, timeout=30)
        try:
            rows = conn.execute(
                f"SELECT CrystalName FROM mainTable WHERE {where}"
            ).fetchall()
        finally:
            conn.close()
        return {row[0] for row in rows}

    def update_data_source_bulk(self, db_dicts, database_path):
        # db_dicts is already restricted to updatable_crystals(), so the bulk
        # update only needs to match on CrystalName.
        keyed = sorted(db_dicts, key=lambda d: tuple(sorted(d)))

        conn = sqlite3.connect(database_path, timeout=30)
        try:
            cursor = conn.cursor()
            for keys, group in groupby(keyed, key=lambda d: tuple(sorted(d))):
                columns = [k for k in keys if k != "CrystalName"]
                sql = (
                    "UPDATE mainTable SET "
                    + ", ".join([f"{col} = :{col}" for col in columns])
                    + " WHERE CrystalName = :CrystalName"
                )
                cursor.executemany(sql, list(group))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_data_source(self, db_dict, dtag, database_path):
        sql = (
            "UPDATE mainTable SET "
            + ", ".join([f"{k} = :{k}" for k in db_dict])
            + f" WHERE CrystalName = '{dtag}'"
            + " AND RefinementOutcome IS NULL"
        )
        conn = sqlite3.connect(database_path, timeout=60)
        # conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        cursor.execute(sql, db_dict)
        conn.commit()
        conn.close()

    def sync_rows_from_master(self, master_path, copy_path, table):
        conn = sqlite3.connect(copy_path)
        conn.execute(f"ATTACH DATABASE '{master_path}' AS master")

        # Insert only rows from master that don't already exist in the copy
        conn.execute(f"""
            INSERT OR IGNORE INTO {table}
            SELECT * FROM master.{table}
        """)

        conn.commit()
        conn.execute("DETACH DATABASE master")
        conn.close()

    def sync_schema_from_master(self, db_master, db_copy, table):
        """Add any columns present in master but missing from copy.
        Preserves column type from master."""

        with sqlite3.connect(db_master) as master_conn:
            master_col_defs = {
                row[1]: row[2]
                for row in master_conn.execute(f"PRAGMA table_info({table})")
            }

        with sqlite3.connect(db_copy) as copy_conn:
            copy_cols = {
                row[1] for row in copy_conn.execute(f"PRAGMA table_info({table})")
            }

            new_cols = set(master_col_defs.keys()) - copy_cols
            for col in new_cols:
                col_type = master_col_defs[col]
                copy_conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")

            copy_conn.commit()

    def safe_symlink(self, src, dst):
        try:
            if os.path.islink(dst) or os.path.exists(dst):
                os.remove(dst)
            os.symlink(src, dst)
        except Exception as e:
            self.log.error(f"Error creating symlink from {src} to {dst}: {e}")

    def export_pipedream_files(self, dataset_dir, compound_code, pipedream_dir, dtag):
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
            self.safe_symlink(target_cif, symlink_cif)
        else:
            self.log.info(f"Could not export restraints files for {dataset_dir}")

        mtz_file_dest = (
            pipedream_dir / dtag / f"postrefine-{compound_code}" / "refine.mtz"
        )
        postrefine_pdb = (
            pipedream_dir / dtag / f"postrefine-{compound_code}" / "refine.pdb"
        )

        self.safe_symlink(postrefine_pdb, dataset_dir / "refine.pdb")
        self.safe_symlink(mtz_file_dest, dataset_dir / "refine.mtz")
        self.safe_symlink(postrefine_pdb, dataset_dir / "refine.split.bound-state.pdb")

        self.safe_symlink(
            pipedream_dir / dtag / f"postrefine-{compound_code}" / "refine_2fofc.map",
            dataset_dir / f"{dtag}_2fofc.map",
        )
        self.safe_symlink(
            pipedream_dir / dtag / f"postrefine-{compound_code}" / "refine_fofc.map",
            dataset_dir / f"{dtag}_fofc.map",
        )

    def pipedream_refinement_metrics(
        self, pipedream_model, compound_code, db_timestamp
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

        if not mmcif_model.exists():
            self.log.warning(f"BUSTER model CIF not found at {mmcif_model}")
        if not mmcif_reflections.exists():
            self.log.warning(f"BUSTER reflections CIF not found at {mmcif_reflections}")

        return {
            "RefinementResolution": resolution,
            "RefinementResolutionTL": self.traffic_light(resolution, 2.0, 2.5),
            "RefinementRcryst": r,
            "RefinementRcrystTraficLight": self.traffic_light(r, 0.20, 0.25),
            "RefinementRfree": rfree,
            "RefinementRfreeTraficLight": self.traffic_light(rfree, 0.25, 0.30),
            "RefinementOutcomePerson": "gda2",
            "RefinementOutcomeDate": db_timestamp,
            "RefinementPDB_latest": str(pipedream_model),
            "RefinementMTZ_latest": str(postrefine_dir / "refine.mtz"),
            "RefinementMMCIFmodel_latest": str(mmcif_model),
            "RefinementMMCIFreflections_latest": str(mmcif_reflections),
            "RefinementMolProbityScore": molprob,
            "RefinementMolProbityScoreTL": self.traffic_light(molprob, 2, 3),
            "RefinementRamachandranOutliers": rama_out,
            "RefinementRamachandranOutliersTL": self.traffic_light(rama_out, 0.3, 1),
            "RefinementRamachandranFavored": rama_fav,
            "RefinementRamachandranFavoredTL": self.traffic_light(
                rama_fav, 98, 95, reverse=True
            ),
            "RefinementRmsdBonds": rmsd_bonds,
            "RefinementRmsdBondsTL": self.traffic_light(rmsd_bonds, 0.012, 0.018),
            "RefinementRmsdAngles": rmsd_angles,
            "RefinementRmsdAnglesTL": self.traffic_light(rmsd_angles, 1.5, 2.0),
            "RefinementStatus": "finished",
            "RefinementBusterReportHTML": str(report),
            "RefinementDate": db_timestamp,
        }

    def traffic_light(self, value, green, orange=None, reverse=False):
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
