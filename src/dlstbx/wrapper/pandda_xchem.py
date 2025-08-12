from __future__ import annotations

import subprocess

# import molviewspec as mvs
from dlstbx.wrapper import Wrapper


class LigandFitWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.pandda_xchem"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.debug(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        params = self.recwrap.recipe_step["job_parameters"]

        job_type = params.get("job_type")
        CompoundSMILES = params.get("CompoundSMILES")
        processing_dir = params.get("processing_dir")
        dtag = params.get("dtag")

        # working_directory = pathlib.Path(params["working_directory"])
        # working_directory.mkdir(parents=True, exist_ok=True)
        # results_directory = pathlib.Path(params["results_directory"])
        # results_directory.mkdir(parents=True, exist_ok=True)
        analysis_dir = processing_dir / "analysis"
        model_dir = analysis_dir / "model_building"
        well_dir = model_dir / dtag

        with open(well_dir / "ligand.smi", "w") as smi_file:
            smi_file.write(CompoundSMILES)

        # -------------------------------------------------------
        if job_type == "prep":
            # offer grade and elbow options?
            acedrg_command = f"module load ccp4; acedrg -i {well_dir / 'ligand.smi'} -o {well_dir / 'lig'}"

            try:
                result = subprocess.run(
                    acedrg_command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    cwd=well_dir,
                    check=True,
                    timeout=params.get("timeout-minutes")
                    * 60,  # have seperate timeouts?
                )

            except subprocess.CalledProcessError as e:
                self.log.error(
                    f"Ligand restraint generation command: '{acedrg_command}' failed"
                )
                self.log.info(e.stdout)
                self.log.error(e.stderr)
                return False

            with open(well_dir / "acedrg.log", "w") as log_file:
                log_file.write(result.stdout)

        # -------------------------------------------------------
        if job_type == "single":
            pandda2_command = f"source /dls/data2temp01/labxchem/data/2017/lb18145-17/processing/edanalyzer/act; \
            conda activate /dls/science/groups/i04-1/conor_dev/pandda_2_gemmi/env_pandda_2; \
            python -u /dls/science/groups/i04-1/conor_dev/pandda_2_gemmi/scripts/process_dataset.py --data_dirs={model_dir} --out_dir={analysis_dir / 'panddas'} --dtag={dtag}"

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
        if job_type == "prerun":
            pandda2_command = f"source /dls/data2temp01/labxchem/data/2017/lb18145-17/processing/edanalyzer/act; \
            conda activate /dls/science/groups/i04-1/conor_dev/pandda_2_gemmi/env_pandda_2; \
            python -u /dls/science/groups/i04-1/conor_dev/pandda_2_gemmi/scripts/process_dataset.py --data_dirs={model_dir} --out_dir={analysis_dir / 'panddas'}"

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
        if job_type == "postrun":
            pandda2_command = f"source /dls/data2temp01/labxchem/data/2017/lb18145-17/processing/edanalyzer/act; \
            conda activate /dls/science/groups/i04-1/conor_dev/pandda_2_gemmi/env_pandda_2; \
            python -u /dls/science/groups/i04-1/conor_dev/pandda_2_gemmi/scripts/postrun.py --data_dirs={model_dir} --out_dir={analysis_dir / 'panddas'} --use_ligand_data=False --debug=True --local_cpus=36"

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
                self.log.error(f"PanDDA2 postrun command: '{pandda2_command}' failed")
                self.log.info(e.stdout)
                self.log.error(e.stderr)
                return False

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

        self.log.info("Auto PanDDApipeline finished successfully")
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
