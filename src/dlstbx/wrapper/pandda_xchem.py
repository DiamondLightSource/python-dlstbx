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
        # self.log.info((f"SLURM_ARRAY_TASK_ID: {slurm_task_id}"))
        params = self.recwrap.recipe_step["job_parameters"]

        # database_path = Path(params.get("database_path"))
        processing_dir = Path(params.get("processing_directory"))
        analysis_dir = Path(processing_dir / "analysis")
        model_dir = Path(params.get("model_directory"))
        auto_panddas_dir = Path(analysis_dir / "auto_pandda2")
        Path(auto_panddas_dir).mkdir(exist_ok=True)

        n_datasets = params.get("n_datasets")
        self.log.info(f"N_datasets: {n_datasets}")
        if n_datasets > 1:
            with open(model_dir / "datasets.json", "r") as f:
                datasets = json.load(f)
                dtag = datasets[int(slurm_task_id) - 1]
        else:
            dtag = params.get("dtag")

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
                f"Multiple .smiles files found in in {compound_dir}:, {smiles_files}, warning for dtag {dtag}"
            )
        else:
            smiles_file = smiles_files[0]
            CompoundCode = smiles_file.stem

        smiles_file = next(Path(compound_dir).rglob("*.smiles"), None)
        CompoundCode = smiles_file.stem

        # -------------------------------------------------------
        # acedrg_command = f"module load ccp4; acedrg -i {smiles_file} -o {CompoundCode}"
        restraints_command = f"module load buster; module load graphviz; \
                               export CSDHOME=/dls_sw/apps/CSDS/2024.1.0/; export BDG_TOOL_MOGUL=/dls_sw/apps/CSDS/2024.1.0/ccdc-software/mogul/bin/mogul; \
                               grade2 --in {smiles_file} --itype smi --out {CompoundCode} -f; "

        try:
            result = subprocess.run(
                restraints_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=compound_dir,
                check=True,
                timeout=params.get("timeout-minutes") * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(
                f"Ligand restraint generation command: '{restraints_command}' failed for dataset {dtag}"
            )

            self.log.info(e.stdout)
            self.log.error(e.stderr)
            return False

        restraints = compound_dir / f"{CompoundCode}.restraints.cif"
        restraints.rename(compound_dir / f"{CompoundCode}.cif")
        pdb = compound_dir / f"{CompoundCode}.xyz.pdb"
        pdb.rename(compound_dir / f"{CompoundCode}.pdb")

        with open(dataset_dir / "restraints.log", "w") as log_file:
            log_file.write(result.stdout)

        self.log.info(f"Restraints generated succesfully for dtag {dtag}")

        pandda2_command = f"source /dls_sw/i04-1/software/PanDDA2/venv/bin/activate; \
        python -u /dls_sw/i04-1/software/PanDDA2/scripts/process_dataset.py --data_dirs={model_dir} --out_dir={auto_panddas_dir} --dtag={dtag}"

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

        pandda_log = auto_panddas_dir / f"processed_datasets/{dtag}" / "pandda2.log"
        with open(pandda_log, "w") as log_file:
            log_file.write(result.stdout)

        # -------------------------------------------------------
        # Integrate back with XCE via datasource
        # db_dict = {}
        # db_dict["DimplePANDDAwasRun"] = True
        # # db_dict["DimplePANDDAreject"] = False
        # db_dict["DimplePANDDApath"] = str(auto_panddas_dir / "processed_datasets")

        # try:
        #     self.update_data_source(db_dict, dtag, database_path)
        #     self.log.info(f"Updated sqlite database for dataset {dtag}")
        # except Exception as e:
        #     self.log.info(f"Could not update sqlite database for dataset {dtag}: {e}")

        self.log.info("Auto PanDDA2 pipeline finished successfully")
        return True

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
