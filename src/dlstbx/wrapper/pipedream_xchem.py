from __future__ import annotations

import sqlite3
import subprocess
from pathlib import Path

from dlstbx.wrapper import Wrapper


class PipedreamWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.pipedream_xchem"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.info(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        params = self.recwrap.recipe_step["job_parameters"]

        # database_path = Path(params.get("database_path"))
        processed_dir = Path(params.get("processed_directory"))
        analysis_dir = Path(processed_dir / "analysis")
        model_dir = Path(params.get("model_directory"))
        dtag = params.get("dtag")

        dataset_dir = model_dir / dtag
        pipedream_dir = analysis_dir / "pipedream"
        Path(pipedream_dir).mkdir(parents=True, exist_ok=True)
        out_dir = pipedream_dir / dtag
        upstream_mtz = params.get("upstream_mtz")
        dimple_pdb = dataset_dir / "dimple.pdb"
        dimple_mtz = dataset_dir / "dimple.mtz"

        self.log.info(f"Processing dtag: {dtag}")
        self.process_pdb_file(dimple_pdb)

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
            self.log.error(f"Pipedream command: '{pipedream_command}' failed")
            self.log.info(e.stdout)
            self.log.error(e.stderr)
            return False

        self.log.info(f"Pipedream finished successfully for dtag {dtag}")
        return True

    def process_pdb_file(self, dimple_pdb: Path, dtag: str):
        self.log.info(f"Removing crystallisation components from pdb file for {dtag}")

        if dimple_pdb.exists():
            with open(dimple_pdb, "r", encoding="utf-8") as f:
                lines = f.readlines()

            # Count removals by component type
            original_count = len(lines)
            components_to_remove = ["DMS", "EDO", "GOL", "SO4", "PO4", "PEG"]
            removed_counts = dict.fromkeys(components_to_remove, 0)

            kept_lines = []
            for line in lines:
                if any(res in line for res in components_to_remove):
                    # Count which component was found
                    for comp in components_to_remove:
                        if comp in line:
                            removed_counts[comp] += 1
                            break
                else:
                    kept_lines.append(line)

            # Write cleaned file
            with open(dimple_pdb, "w", encoding="utf-8") as f:
                f.writelines(kept_lines)

            removed_total = original_count - len(kept_lines)
            if removed_total > 0:
                component_summary = ", ".join(
                    [
                        f"{comp}: {count}"
                        for comp, count in removed_counts.items()
                        if count > 0
                    ]
                )
                self.log.debug(
                    f"Removed {removed_total} lines from {dtag} ({component_summary})"
                )

        else:
            self.log.debug(f"Dimple pdb {dimple_pdb} does not exist")
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
