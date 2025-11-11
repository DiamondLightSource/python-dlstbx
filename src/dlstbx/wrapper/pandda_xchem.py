from __future__ import annotations

import json
import os
import sqlite3
import subprocess
from pathlib import Path

# import molviewspec as mvs
from dlstbx.wrapper import Wrapper


class PanDDAWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.pandda_xchem"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.info(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        slurm_task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
        self.log.info((f"SLURM_ARRAY_TASK_ID: {slurm_task_id}"))
        params = self.recwrap.recipe_step["job_parameters"]

        database_path = Path(params.get("database_path"))
        processing_dir = Path(params.get("processing_directory"))
        analysis_dir = Path(processing_dir / "analysis")
        model_dir = Path(params.get("model_directory"))

        auto_panddas_dir = Path(analysis_dir / "auto_pandda2")
        Path(auto_panddas_dir).mkdir(exist_ok=True)

        datasets = json.loads(params.get("datasets"))
        dtag = datasets[int(slurm_task_id) - 1]
        self.log.info(f"Processing dtag: {dtag}")
        dataset_dir = model_dir / dtag
        compound_dir = dataset_dir / "compound"

        smiles_files = list(compound_dir.glob("*.smiles"))

        if len(smiles_files) == 0:
            self.log.error(
                f"No .smiles file present in {compound_dir}, cannot continue for dtag {dtag}"
            )
            return False
        elif len(smiles_files) > 1:
            self.log.error(
                f"Multiple .smiles files found in in {compound_dir}:, {smiles_files}, cannot continue for dtag {dtag}"
            )
        else:
            smiles_file = smiles_files[0]
            CompoundCode = smiles_file.stem

        smiles_file = next(Path(compound_dir).rglob("*.smiles"), None)

        # working_directory = pathlib.Path(params["working_directory"])
        # working_directory.mkdir(parents=True, exist_ok=True)

        db_dict = {}  # store results to integrate back with soakDB?

        # -------------------------------------------------------
        acedrg_command = f"module load ccp4; acedrg -i {smiles_file} -o {CompoundCode}"

        try:
            result = subprocess.run(
                acedrg_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=compound_dir,
                check=True,
                timeout=params.get("timeout-minutes") * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(
                f"Ligand restraint generation command: '{acedrg_command}' failed for dataset {dtag}"
            )

            self.log.info(e.stdout)
            self.log.error(e.stderr)
            return False

        with open(dataset_dir / "acedrg.log", "w") as log_file:
            log_file.write(result.stdout)

        pandda2_command = f"source /dls_sw/i04-1/software/PanDDA2/venv/bin/activate; \
        python -u /dls_sw/i04-1/software/PanDDA2/scripts/process_dataset.py --data_dirs={model_dir} --out_dir={auto_panddas_dir} --dtag={dtag} > {dataset_dir / 'pandda2.log'}"

        # pandda2_command = f"source /dls/data2temp01/labxchem/data/2017/lb18145-17/processing/edanalyzer/act; conda activate /dls/science/groups/i04-1/conor_dev/pandda_2_gemmi/env_pandda_2; \
        # python -u /dls/science/groups/i04-1/conor_dev/pandda_2_gemmi/scripts/process_dataset.py  --data_dirs={model_dir} --out_dir={auto_panddas_dir} --dtag={dtag} > {dataset_dir / 'pandda2.log'}"

        try:
            result = subprocess.run(
                pandda2_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=dataset_dir,
                check=True,
                timeout=params.get("timeout-minutes") * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(f"PanDDA2 command: '{pandda2_command}' failed")
            self.log.info(e.stdout)
            self.log.error(e.stderr)
            return False

        # -------------------------------------------------------
        # Integrate back with XCE via datasource
        # db_dict["DimplePANDDAwasRun"] = True
        # # db_dict["DimplePANDDAreject"] = False
        # db_dict["DimplePANDDApath"] = str(auto_panddas_dir / "processed_datasets")

        # try:
        #     self.update_data_source(db_dict, dtag, database_path)
        #     self.log.info(f"Updated sqlite database for dataset {dtag}")
        # except Exception as e:
        #     self.log.info(f"Could not update sqlite database for dataset {dtag}: {e}")

        # json results for synchweb tables
        # data = [["PanDDA dataset", "CompoundSMILES", "result"],[f"{dtag}", f"{CompoundSMILES}", f"{}"]]
        # with open(analysis_dir / "pandda_results.json", "w") as f:
        #     json.dump(data, f)

        self.log.info("Sending results to ISPyB")
        self.send_attachments_to_ispyb(dataset_dir)

        self.log.info("Auto PanDDA2 pipeline finished successfully")
        return True

    def send_attachments_to_ispyb(self, dataset_dir):
        for f in dataset_dir.iterdir():
            if f.suffix == ".json":
                file_type = "Result"
                importance_rank = 1
            elif f.suffix == ".log":
                file_type = "Log"
                importance_rank = 2
            else:
                continue
            try:
                result_dict = {
                    "file_path": str(dataset_dir),
                    "file_name": f.name,
                    "file_type": file_type,
                    "importance_rank": importance_rank,
                }
                self.record_result_individual_file(result_dict)
                self.log.info(f"Uploaded {f.name} as an attachment")
            except Exception:
                self.log.warning(f"Could not attach {f.name} to ISPyB", exc_info=True)

    def update_data_source(self, db_dict, dtag, database_path):
        sql = (
            "UPDATE mainTable SET "
            + ", ".join([f"{k} = :{k}" for k in db_dict])
            + f" WHERE CrystalName = '{dtag}'"
        )
        conn = sqlite3.connect(database_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        cursor.execute(sql, db_dict)
        conn.commit()
