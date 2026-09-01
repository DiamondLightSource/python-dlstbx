from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from dlstbx.util.pipedream_xchem_helpers import (
    cleanup_setvar_files,
    write_pipedream_parameters,
)
from dlstbx.util.soakdb import prepare_auto_db, updatable_crystals
from dlstbx.util.xchem_collate_helpers import (
    symlink_score_buckets,
    update_xchem_database,
)
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
        # Perform model selection (PanDDA2 | Pipedream) & re-integrate into XChem environment

        try:
            db_copy = prepare_auto_db(processing_dir)
            updatable = updatable_crystals(db_copy, overwrite)
        except Exception as e:
            self.log.error(f"Could not prepare auto db for {processing_dir}: {e}")
            db_copy, updatable = None, None

        # Score bucketing reads only the PanDDA events csv, so it runs whether or
        # not the soakDB copy could be prepared.
        try:
            symlink_score_buckets(panddas_dir, pandda_dir, self.log)
        except Exception as e:
            self.log.error(f"Exception bucketing scores for {panddas_dir}: {e}")

        if updatable is not None:
            try:
                update_xchem_database(
                    model_dir, pipedream_dir, panddas_dir, db_copy, updatable, self.log
                )
            except Exception as e:
                self.log.error(f"Exception updating database for {processing_dir}: {e}")

        # -------------------------------------------------------
        # Perform Pipedream collate --> html output
        if pipedream:
            xchem_python = (
                "/dls/science/groups/i04-1/software/micromamba/envs/xchem/bin/python"
            )
            pipedream_command = f"{xchem_python} /dls/science/groups/i04-1/software/pipedream_xchem/collate_pipedream_results.py \
            --input {pipedream_dir / 'Pipedream_output.json'}  --output-dir {pipedream_dir / 'Pipedream_results'} --no-browser --no-plots -v"

            self.log.info(f"Running Collate command: {pipedream_command}")

            try:
                subprocess.run(
                    pipedream_command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    cwd=pipedream_dir,
                    check=True,
                    timeout=params.get("timeout-minutes") * 60,
                )

            except subprocess.CalledProcessError as e:
                self.log.error(
                    f"Pipedream collate command failed (exit {e.returncode})\n"
                    f"--- stdout ---\n{e.stdout}\n--- stderr ---\n{e.stderr}"
                )

            try:
                write_pipedream_parameters(
                    processing_dir, pipedream_dir, logger=self.log
                )
            except Exception as e:
                self.log.error(
                    f"Could not write pipedream parameters for {pipedream_dir}: {e}"
                )

        else:
            self.log.info(
                f"Skipping collation of Pipedream results for {pipedream_dir}"
            )

        # -------------------------------------------------------
        # Perform XChemAlign collate step
        xca_dir = processing_dir / "analysis" / "xchemalign"
        config = xca_dir / "config.yaml"
        assemblies = xca_dir / "assemblies.yaml"

        if not config.exists() or not assemblies.exists():
            self.log.info(
                f"No config/assemblies .yaml in {str(xca_dir)}, skipping autoXCA"
            )
        else:
            autoxca_dir = auto_dir / "xchemalign"
            autoxca_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy(config, autoxca_dir / "config.yaml")
            shutil.copy(assemblies, autoxca_dir / "assemblies.yaml")

            xca_python = "/dls/science/groups/i04-1/software/xchem-align/env_xchem_align/bin/python"
            xca_command = f"{xca_python} -m xchemalign.collator -d {autoxca_dir} && \
            {xca_python} -m xchemalign.aligner -d {autoxca_dir}"

            self.log.info(f"Running XCA command: {xca_command}")

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
                self.log.error(
                    f"XCA command failed (exit {e.returncode})\n"
                    f"--- stdout ---\n{e.stdout}\n--- stderr ---\n{e.stderr}"
                )

        # Clean up orphaned autoBUSTER setvar logs left in the pipedream dir
        try:
            cleanup_setvar_files(pipedream_dir, self.log)
        except Exception as e:
            self.log.error(f"Could not clean up setvar logs in {pipedream_dir}: {e}")

        self.log.info("Auto XChemCollate finished successfully")
        return True
