from __future__ import annotations

import sqlite3
import subprocess
from pathlib import Path

from dlstbx.wrapper import Wrapper


class PipedreamWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.pipedream"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.info(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        params = self.recwrap.recipe_step["job_parameters"]

        # database_path = Path(params.get("database_path"))
        processed_dir = Path(params.get("processed_directory"))
        analysis_dir = Path(processed_dir / "analysis")
        upstream_mtz = params.get("upstream_mtz")
        dimple_pdb = ""
        dimple_mtz = ""
        out_dir = analysis_dir / "Pipedream" / dtag

        dtag = params.get("dtag")

        self.log.info(f"Processing dtag: {dtag}")

        pipedream_command = f"module load buster; module load graphviz; \
            export BDG_TOOL_MOGUL=/dls_sw/apps/CSDS/2024.1.0/ccdc-software/mogul/bin/mogul; \
            /dls_sw/apps/GPhL/BUSTER/20250717/scripts/pipedream \
            -nolmr \
            -hklin {upstream_mtz} \
            -xyzin {dimple_pdb} \
            -hklref {dimple_mtz} \
            -d {out_dir} \
            -mrefine TLSbasic,WaterUpdatePkmaps \
            -keepwater \
            -remediate \
            -sidechainrebuild \
            -runpepflip \
            -rhocommands \
            -xclusters \
            -nochirals "
        # -rhofit ligand.cif

        try:
            result = subprocess.run(
                pipedream_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=analysis_dir,
                check=True,
                timeout=params.get("timeout-minutes") * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(f"PanDDA2 command: '{pipedream_command}' failed")
            self.log.info(e.stdout)
            self.log.error(e.stderr)
            return False

        self.log.info(f"Pipedream finished successfully for dtag {dtag}")
        return True

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
