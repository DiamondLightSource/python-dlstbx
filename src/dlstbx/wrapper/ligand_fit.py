from __future__ import annotations

import math
import os
import pathlib
import re
import shutil
import subprocess

from iotbx import pdb

from dlstbx.wrapper import Wrapper

class LigandFitWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.ligand_fit"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.debug(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )
        # Get parameters from the recipe file
        params = self.recwrap.recipe_step["job_parameters"] #dictionary
        pdb_file = params["pdb"]

        phenix_command = f"phenix.ligandfit -xx "
        result = subprocess.run(
        phenix_command, shell=True, capture_output=True, text=True
        )
        return True
