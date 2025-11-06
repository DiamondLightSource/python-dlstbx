from __future__ import annotations

import sqlite3
import subprocess
from pathlib import Path

from dlstbx.wrapper import Wrapper


class PanDDApostWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.pandda_xchem"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.info(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        params = self.recwrap.recipe_step["job_parameters"]
        processing_dir = Path(params.get("processing_directory"))
        analysis_dir = Path(processing_dir / "analysis")
        model_dir = Path(params.get("model_directory"))

        auto_panddas_dir = Path(analysis_dir / "panddas_auto")
        Path(auto_panddas_dir).mkdir(exist_ok=True)

        db_dict = {}  # store results to integrate back with soakDB

        # -------------------------------------------------------
        pandda2_command = f"source /dls_sw/i04-1/software/PanDDA2/venv/bin/activate; \
        python -u /dls_sw/i04-1/software/PanDDA2/pandda_gemmi/pandda/postrun.py --data_dirs={model_dir} --out_dir={auto_panddas_dir} --use_ligand_data=False --debug=True --local_cpus=4 > {auto_panddas_dir / 'pandda2_postrun.log'}"

        try:
            result = subprocess.run(
                pandda2_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=auto_panddas_dir,
                check=True,
                timeout=params.get("timeout-minutes") * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(f"PanDDA2_post command: '{pandda2_command}' failed")
            self.log.info(e.stdout)
            self.log.error(e.stderr)
            return False

        # -------------------------------------------------------

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
