from __future__ import annotations

import shutil
import sqlite3
import subprocess
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
        pandda2_command = f"source /dls_sw/i04-1/software/PanDDA2/venv/bin/activate; \
        python -u /dls_sw/i04-1/software/PanDDA2/scripts/postrun.py --data_dirs={model_dir} --out_dir={panddas_dir} --use_ligand_data=False --debug=True --local_cpus=1 > {panddas_dir / 'pandda2_postrun.log'}"

        self.log.info(f"Running XChemCollate command: {pandda2_command}")

        try:
            result = subprocess.run(
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
        # Re-integrate into XChem environment

        db_dict = {}
        db_master = processing_dir / "database" / "soakDBDataFile.sqlite"
        db_copy = processing_dir / "auto/database" / "autosoakDBDataFile.sqlite"

        # use a copy of the main database
        if not db_copy.exists():
            Path(db_copy.parents[0]).mkdir(parents=True, exist_ok=True)
            shutil.copy(db_master, db_copy)

        # update copy with any new rows
        self.sync_new_rows_from_master(db_master, db_copy, "mainTable")

        # perform model selection
        for dir in model_dir.iterdir():
            dtag = dir.name
            pipedream_model = self.find_pipedream_model(
                pipedream_dir, dtag, rscc_thresh=0.7
            )
            pandda_model = self.find_pandda_model(panddas_dir, dtag)

            if pipedream_model:  # if pipedream model of sufficient quality, take it
                db_dict["RefinementBoundConformation"] = str(pipedream_model)
                db_dict["RefinementOutcome"] = "3 - In Refinement"
                self.log.info(f"Selected Pipedream model for {dtag}")
            elif pandda_model:
                db_dict["RefinementBoundConformation"] = str(pandda_model)
                db_dict["RefinementOutcome"] = "2 - PANDDA model"
                self.log.info(f"Selected PanDDA2 model for {dtag}")
            else:
                db_dict["RefinementOutcome"] = "7 - Analysed & Rejected"
                self.log.info(f"No model selected for {dtag}")

            if db_dict:
                try:
                    self.update_data_source(db_dict, dtag, db_copy)
                    print(f"Updated sqlite database for dataset {dtag}")
                except Exception as e:
                    print(f"Could not update sqlite database for dataset {dtag}: {e}")

        # -------------------------------------------------------
        # Perform Pipedream collate --> html output
        if pipedream:
            pipedream_command = f"source /dls/science/groups/i04-1/software/XChem/xchempaths.sh; micromamba init; micromamba activate xchem; \
            python /dls/science/groups/i04-1/software/pipedream_xchem/collate_pipedream_results.py --input {pipedream_dir / 'Pipedream_output.json'} --no-browser --no-plots -v"

            self.log.info(f"Running XChemCollate command: {pipedream_command}")

            try:
                result = subprocess.run(
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

        self.log.info("Auto XChemCollate finished successfully")
        return True

    def find_pipedream_model(self, pipedream_dir, dtag, rscc_thresh) -> Path | None:
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
        return refine_pdb if refine_pdb.exists() and rscc > rscc_thresh else None

    def find_pandda_model(self, panddas_dir, dtag) -> Path | None:
        pandda_dataset_dir = panddas_dir / "processed_datasets" / f"{dtag}"
        pandda_model = (
            pandda_dataset_dir / "modelled_structures" / f"{dtag}-pandda-model.pdb"
        )
        model_path = pandda_model if pandda_model.exists() else None
        return model_path

    def update_data_source(self, db_dict, dtag, database_path):
        sql = (
            "UPDATE mainTable SET "
            + ", ".join([f"{k} = :{k}" for k in db_dict])
            + f" WHERE CrystalName = '{dtag}'"
        )
        conn = sqlite3.connect(database_path)
        # conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        cursor.execute(sql, db_dict)
        conn.commit()
        conn.close()

    def sync_new_rows_from_master(self, master_path, copy_path, table_name):
        conn = sqlite3.connect(copy_path)
        conn.execute(f"ATTACH DATABASE '{master_path}' AS master")

        # Insert only rows from master that don't already exist in the copy
        conn.execute(f"""
            INSERT OR IGNORE INTO {table_name}
            SELECT * FROM master.{table_name}
        """)

        conn.commit()
        conn.execute("DETACH DATABASE master")
        conn.close()
