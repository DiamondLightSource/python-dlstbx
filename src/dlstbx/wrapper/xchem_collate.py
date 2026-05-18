from __future__ import annotations

import datetime
import os
import shutil
import sqlite3
import subprocess
from itertools import groupby
from pathlib import Path

from dlstbx.wrapper import Wrapper


class XChemCollateWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.xchem_collate"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.info(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        params = self.recwrap.recipe_step["job_parameters"]
        pipedream = params.get("pipedream")
        processing_dir = Path(params.get("processing_directory"))
        auto_dir = processing_dir / "auto"
        analysis_dir = auto_dir / "analysis"
        pandda_dir = analysis_dir / "pandda2"
        model_dir = pandda_dir / "model_building"
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
        # Perform model selection (pandda/pipedream) & re-integrate into XChem environment

        try:
            self.update_xchem_database(
                processing_dir, model_dir, pipedream_dir, panddas_dir
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
        # Perform XChemAlign collate
        xca_dir = analysis_dir / "xchem_align"
        config = xca_dir / "config.yaml"
        assemblies = xca_dir / "assemblies.yaml"

        if not config.exists() or not assemblies.exists():
            self.log.info(
                f"No config/assemblies .yaml in {str(xca_dir)}, skipping autoXCA"
            )
        else:
            autoxca_dir = auto_dir / "xchem_align"  # do relative paths work from here?
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

    def find_pipedream_model(self, pipedream_dir, dtag, rscc_thresh):
        RHOFIT_HIT_LOG = "Hit_corr.log"

        dataset_dir = pipedream_dir / dtag
        rhofit_dir = next(dataset_dir.glob("rhofit-*"), None)
        postrefine_dir = next(dataset_dir.glob("postrefine-*"), None)

        if not rhofit_dir:
            return None

        if not postrefine_dir:
            return None

        hit_log = rhofit_dir / RHOFIT_HIT_LOG
        if not hit_log.exists():
            return None

        with open(hit_log) as f:
            rscc = max(float(line.split()[1]) for line in f if line.strip())

        refine_pdb = postrefine_dir / "refine.pdb"
        return (
            (refine_pdb, rscc)
            if refine_pdb.exists() and rscc > rscc_thresh
            else (None, None)
        )

    def find_pandda_model(self, panddas_dir, dtag) -> Path | None:
        pandda_dataset_dir = panddas_dir / "processed_datasets" / f"{dtag}"
        pandda_model = (
            pandda_dataset_dir / "modelled_structures" / f"{dtag}-pandda-model.pdb"
        )
        model_path = pandda_model if pandda_model.exists() else None
        return model_path

    def update_xchem_database(
        self, processing_dir, model_dir, pipedream_dir, panddas_dir
    ):
        """Exports results to SoakDB database"""

        db_master = processing_dir / "database" / "soakDBDataFile.sqlite"
        db_copy = processing_dir / "auto/database" / "autosoakDBDataFile.sqlite"

        if not db_copy.exists():
            Path(db_copy.parents[0]).mkdir(parents=True, exist_ok=True)
            shutil.copy(db_master, db_copy)

        self.sync_schema_from_master(db_master, db_copy, "mainTable")
        self.sync_rows_from_master(db_master, db_copy, "mainTable")

        # Build list of update dicts for batch update
        db_dicts = []
        for dir in model_dir.iterdir():  # or iterate through results.json entries
            dtag = dir.name
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

                db_dicts.append(
                    {
                        "CrystalName": dtag,
                        "RefinementBoundConformation": str(pipedream_model),
                        "RefinementOutcome": RefinementOutome,
                        "RefinementLigandConfidence": RefinementLigandConfidence,
                        "RefinementLigandCC": rscc,
                        # "RefinementCIF": str(model_dir / dtag / compound /)
                        "RefinementCIFprogram": "Grade2",
                        "LastUpdated": db_timestamp,
                        "LastUpdated_by": "gda2",
                    }
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

        try:
            self.update_data_source_bulk(db_dicts, db_copy)
            self.log.debug(f"Bulk updated {db_copy} for {len(db_dicts)} datasets")
        except Exception as e:
            self.log.debug(f"Could not bulk update {db_copy}: {e}")

    def update_data_source_bulk(self, db_dicts, database_path):
        # Group dicts that share the same columns together
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
                    + " AND RefinementOutcome IS NULL"
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
