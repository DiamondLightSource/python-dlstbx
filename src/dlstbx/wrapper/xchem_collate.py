from __future__ import annotations

import subprocess
from pathlib import Path

import yaml

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

        # if panddas_dir.exists():
        #     pandda2_command = f"source /dls_sw/i04-1/software/PanDDA2/venv/bin/activate; \
        #     python -u /dls_sw/i04-1/software/PanDDA2/scripts/postrun.py --data_dirs={model_dir} --out_dir={panddas_dir} --use_ligand_data=False --debug=True --local_cpus=4 > {panddas_dir / 'pandda2_postrun.log'}"

        #     self.log.info(f"Running XChemCollate command: {pandda2_command}")

        #     try:
        #         subprocess.run(
        #             pandda2_command,
        #             shell=True,
        #             capture_output=True,
        #             text=True,
        #             cwd=panddas_dir,
        #             check=True,
        #             timeout=params.get("timeout-minutes") * 60,
        #         )

        #     except subprocess.CalledProcessError as e:
        #         self.log.error(f"XChemCollate command: '{pandda2_command}' failed")
        #         self.log.info(e.stdout)
        #         self.log.error(e.stderr)

        # -------------------------------------------------------
        # Perform model selection (PanDDA2 | Pipedream) & re-integrate into XChem environment

        # try:
        #     db_copy = prepare_auto_db(processing_dir)
        #     updatable = updatable_crystals(db_copy, overwrite)
        # except Exception as e:
        #     self.log.error(f"Could not prepare auto db for {processing_dir}: {e}")
        #     db_copy, updatable = None, None

        # if updatable is not None:
        #     try:
        #         symlink_score_buckets(panddas_dir, pandda_dir, updatable, self.log)
        #     except Exception as e:
        #         self.log.error(f"Exception bucketing scores for {panddas_dir}: {e}")

        #     try:
        #         update_xchem_database(
        #             model_dir, pipedream_dir, panddas_dir, db_copy, updatable, self.log
        #         )
        #     except Exception as e:
        #         self.log.error(f"Exception updating database for {processing_dir}: {e}")

        # -------------------------------------------------------
        # Perform Pipedream collate --> html output
        # if pipedream:
        #     xchem_python = (
        #         "/dls/science/groups/i04-1/software/micromamba/envs/xchem/bin/python"
        #     )
        #     pipedream_command = f"{xchem_python} /dls/science/groups/i04-1/software/pipedream_xchem/collate_pipedream_results.py \
        #     --input {pipedream_dir / 'Pipedream_output.json'}  --output-dir {pipedream_dir / 'Pipedream_results'} --no-browser --no-plots -v"

        #     self.log.info(f"Running Collate command: {pipedream_command}")

        #     try:
        #         subprocess.run(
        #             pipedream_command,
        #             shell=True,
        #             capture_output=True,
        #             text=True,
        #             cwd=pipedream_dir,
        #             check=True,
        #             timeout=params.get("timeout-minutes") * 60,
        #         )

        #     except subprocess.CalledProcessError as e:
        #         self.log.error(
        #             f"Pipedream collate command failed (exit {e.returncode})\n"
        #             f"--- stdout ---\n{e.stdout}\n--- stderr ---\n{e.stderr}"
        #         )

        #     try:
        #         write_pipedream_parameters(
        #             processing_dir, pipedream_dir, logger=self.log
        #         )
        #     except Exception as e:
        #         self.log.error(
        #             f"Could not write pipedream parameters for {pipedream_dir}: {e}"
        #         )

        # else:
        #     self.log.info(
        #         f"Skipping collation of Pipedream results for {pipedream_dir}"
        #     )

        # Clean up orphaned autoBUSTER setvar logs left in the pipedream dir
        # try:
        #     cleanup_setvar_files(pipedream_dir, self.log)
        # except Exception as e:
        #     self.log.error(f"Could not clean up setvar logs in {pipedream_dir}: {e}")

        # -------------------------------------------------------
        # Perform XChemAlign collate step
        # config.yaml/assemblies.yaml may sit at the top of the xchemalign dir
        # or inside the latest upload (upload-current -> upload-vN).
        # xca_dir = processing_dir / "analysis" / "xchemalign"
        # search_dirs = [xca_dir, xca_dir / "upload-current"]
        # for candidate in search_dirs:
        #     config = candidate / "config.yaml"
        #     assemblies = candidate / "assemblies.yaml"
        #     if config.exists() and assemblies.exists():
        #         break
        # else:
        #     config = assemblies = None

        # if config is None:
        #     self.log.info(
        #         "No config/assemblies .yaml in "
        #         f"{' or '.join(str(d) for d in search_dirs)}, skipping autoXCA"
        #     )
        # else:
        #     autoxca_dir = auto_dir / "xchemalign"
        #     autoxca_dir.mkdir(parents=True, exist_ok=True)

        # # Replicate `xchemalign.setup` so the collator runs non-interactively:
        # # it reads config/assemblies from <dir>/upload-current (a symlink to
        # # upload-v<major>) and needs an upload_1 dir present. Create that
        # # structure ourselves, then drop the real yamls in. upload-v3 matches
        # # xchem-align DATA_FORMAT_VERSION (3.1).
        # upload_current = autoxca_dir / "upload-current"
        # if not upload_current.is_symlink():
        #     (autoxca_dir / "upload-v3" / "upload_1").mkdir(
        #         parents=True, exist_ok=True
        #     )
        #     upload_current.symlink_to("upload-v3", target_is_directory=True)

        # shutil.copy(config, upload_current / "config.yaml")
        # shutil.copy(assemblies, upload_current / "assemblies.yaml")

        autoxca_dir = auto_dir / "xchemalign"
        config = Path(
            "/dls/labxchem/data/lb42888/lb42888-66/processing/auto/xchemalign/upload-current/config.yaml"
        )
        xca_python = (
            "/dls/science/users/qvu59474/softwaresrc/xchem-align/env/bin/python"
        )
        # xca_python = "/dls/science/groups/i04-1/software/xchem-align-staging/env_xchem_align/bin/python"  # staging
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

        # Tarball the aligner's latest upload_N directory and push it to
        # Fragalysis. The aligner nests upload_N inside
        # upload-current -> upload-vN, so tar from there.
        target_name = yaml.safe_load(config.read_text())["target_name"]
        upload_dir = (autoxca_dir / "upload-current").resolve()
        upload_subdirs = [
            p
            for p in upload_dir.glob("upload_*")
            if p.is_dir() and p.name.split("_")[-1].isdigit()
        ]

        if not upload_subdirs:
            self.log.error(
                f"No upload_N directory in {upload_dir}, skipping Fragalysis upload"
            )
        else:
            latest_upload = max(
                upload_subdirs, key=lambda p: int(p.name.split("_")[-1])
            )
            tgz_path = autoxca_dir / f"{target_name}.tgz"
            tar_command = f"tar cvfz {tgz_path} {latest_upload.name}"

            self.log.info(f"Running tar command: {tar_command} (in {upload_dir})")

            try:
                subprocess.run(
                    tar_command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    cwd=upload_dir,
                    check=True,
                    timeout=params.get("timeout-minutes") * 60,
                )

            except subprocess.CalledProcessError as e:
                self.log.error(
                    f"tar command failed (exit {e.returncode})\n"
                    f"--- stdout ---\n{e.stdout}\n--- stderr ---\n{e.stderr}"
                )

            # try:
            #     upload_to_fragalysis(
            #         tgz_path,
            #         target_access_string=processing_dir.parent.name,
            #         logger=self.log,
            #     )
            # except Exception as e:
            #     self.log.error(f"Could not upload {tgz_path} to Fragalysis: {e}")

        self.log.info("Auto XChemCollate finished successfully")
        return True
