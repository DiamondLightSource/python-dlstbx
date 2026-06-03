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
        """Performs collation of PanDDA2 & Pipedream results for a labxchem visit.
        Runs automated model selection and re-integrates results back into soakDB,
        and XChem evironment."""

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
        # Perform XChemAlign collate step
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

        pipdream_dtag = pipedream_dir / dtag
        rhofit_dir = next(pipdream_dtag.glob("rhofit-*"), None)
        postrefine_dir = next(pipdream_dtag.glob("postrefine-*"), None)

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
        panddas_dtag = panddas_dir / "processed_datasets" / f"{dtag}"
        pandda_model = panddas_dtag / "modelled_structures" / f"{dtag}-pandda-model.pdb"
        model_path = pandda_model if pandda_model.exists() else None
        return model_path

    def update_xchem_database(
        self, processing_dir, model_dir, pipedream_dir, panddas_dir
    ):
        """Performs model selection & exports results to XChem SoakDB database"""

        db_master = processing_dir / "database" / "soakDBDataFile.sqlite"
        db_copy = processing_dir / "auto/database" / "autosoakDBDataFile.sqlite"

        if not db_copy.exists():
            Path(db_copy.parents[0]).mkdir(parents=True, exist_ok=True)
            shutil.copy(db_master, db_copy)

        self.sync_schema_from_master(db_master, db_copy, "mainTable")
        self.sync_rows_from_master(db_master, db_copy, "mainTable")

        # Build list of dicts for batch updating rows in SQLite
        db_dicts = []
        for dataset_dir in model_dir.iterdir():
            dtag = dataset_dir.name
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

                db_dicts.append(
                    {
                        "CrystalName": dtag,
                        "RefinementBoundConformation": str(pipedream_model),
                        "RefinementOutcome": RefinementOutome,
                        "RefinementLigandConfidence": RefinementLigandConfidence,
                        "RefinementLigandCC": rscc,
                        # "RefinementCIF":
                        "RefinementCIFprogram": "Grade2",
                        "LastUpdated": db_timestamp,
                        "LastUpdated_by": "gda2",
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
