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

        auto_panddas_dir = Path(analysis_dir / "panddas_auto")
        Path(auto_panddas_dir).mkdir(exist_ok=True)

        datasets = json.loads(params.get("datasets"))
        dtag = datasets[int(slurm_task_id) - 1]
        self.log.info(f"Processing dtag: {dtag}")
        well_dir = model_dir / dtag
        compound_dir = well_dir / "compound"

        # working_directory = pathlib.Path(params["working_directory"])
        # working_directory.mkdir(parents=True, exist_ok=True)
        # results_directory = pathlib.Path(params["results_directory"])
        # results_directory.mkdir(parents=True, exist_ok=True)

        db_dict = {}  # store results to integrate back with soakDB

        # -------------------------------------------------------
        acedrg_command = (
            f"module load ccp4; acedrg -i {compound_dir / 'LIG.smi'} -o {'LIG'}"
        )
        # change to take CompoundCode.smiles!

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

        with open(well_dir / "acedrg.log", "w") as log_file:
            log_file.write(result.stdout)

        pandda2_command = f"source /dls_sw/i04-1/software/PanDDA2/venv/bin/activate; \
        python -u /dls_sw/i04-1/software/PanDDA2/pandda_gemmi/pandda/process_dataset.py --data_dirs={model_dir} --out_dir={auto_panddas_dir} --dtag={dtag} > {model_dir / dtag / 'pandda2.log'}"

        try:
            result = subprocess.run(
                pandda2_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=well_dir,
                check=True,
                timeout=params.get("timeout-minutes") * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(f"PanDDA2 command: '{pandda2_command}' failed")
            self.log.info(e.stdout)
            self.log.error(e.stderr)
            return False

        # -------------------------------------------------------
        # Integrate back with XCE
        db_dict["DimplePANDDAwasRun"] = True
        # db_dict["DimplePANDDAreject"] = False
        db_dict["DimplePANDDApath"] = str(auto_panddas_dir / "processed_datasets")

        try:
            self.update_data_source(db_dict, dtag, database_path)
            self.log.info(f"Updated sqlite database for dataset {dtag}")
        except Exception as e:
            self.log.info(f"Could not update sqlite database for dataset {dtag}: {e}")

        # quick json results for synchweb tables
        # data = [["PanDDA dataset", "CompoundSMILES", "result"],[f"{dtag}", f"{CompoundSMILES}", f"{}"]]
        # with open(analysis_dir / "pandda_results.json", "w") as f:
        #     json.dump(data, f)

        # work in zocalo/tmp and copy results?
        # shutil.copytree(
        #     working_directory,
        #     results_directory,
        #     dirs_exist_ok=True,
        #     ignore=ignore_patterns(".*"),
        # )

        # if params.get("create_symlink"):
        #     dlstbx.util.symlink.create_parent_symlink(
        #         os.fspath(working_directory), params["create_symlink"]
        #     )
        #     dlstbx.util.symlink.create_parent_symlink(
        #         os.fspath(results_directory), params["create_symlink"]
        #     )

        # self.log.info("Sending results to ISPyB")
        # self.send_attachments_to_ispyb(results_directory)

        self.log.info("Auto PanDDA2 pipeline finished successfully")
        return True

    def send_attachments_to_ispyb(self, well_dir):
        for f in well_dir.iterdir():
            if f.stem.endswith("final"):
                file_type = "Result"
                importance_rank = 1
            elif f.suffix == ".json":
                file_type = "Result"
                importance_rank = 1
            elif f.suffix == ".log":
                file_type = "Log"
                importance_rank = 2
            else:
                continue
            try:
                result_dict = {
                    "file_path": str(well_dir),
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
