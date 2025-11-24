from __future__ import annotations

import subprocess
from pathlib import Path

from dlstbx.wrapper import Wrapper


class PanDDApostWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.pandda_post"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.info(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        params = self.recwrap.recipe_step["job_parameters"]
        processing_dir = Path(params.get("processing_directory"))
        analysis_dir = processing_dir / "analysis"
        model_dir = analysis_dir / "auto_model_building"
        auto_panddas_dir = analysis_dir / "auto_pandda2"

        # -------------------------------------------------------
        pandda2_command = f"source /dls_sw/i04-1/software/PanDDA2/venv/bin/activate; \
        python -u /dls_sw/i04-1/software/PanDDA2/scripts/postrun.py --data_dirs={model_dir} --out_dir={auto_panddas_dir} --use_ligand_data=False --debug=True --local_cpus=8 > {auto_panddas_dir / 'pandda2_postrun.log'}"

        self.log.info("Running PanDDA2 command: {pandda2_command}")

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

        self.log.info("Auto PanDDA2-post finished successfully")
        return True
