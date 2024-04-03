# Wrapper for method of using ccp4 scaleit to adjust 2 .mtz file to the same overall scale.
from __future__ import annotations

import os
import subprocess
from pathlib import Path

# from iotbx import mtz
from dlstbx.wrapper import Wrapper

# from argparse import Namespace


class ScaleitWrapper(Wrapper):

    _logger_name = "dlstbx.wrap.scaleit"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.debug(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        # Get parameters from the recipe file
        self.params = self.recwrap.recipe_step["job_parameters"]

        self.working_directory = Path(
            self.recwrap.recipe_step["parameters"]["workingdir"]
        )
        # Make the directories if they don't already exist
        self.working_directory.mkdir(parents=True, exist_ok=True)
        self.log.info(f"Here are the params {self.params}")
        mtz_files = self.params["scaleit"].get("data", [])
        if not mtz_files:
            self.log.error("Could not identify on what data to run")
            return False
        if len(mtz_files) != 2:
            self.log.error(
                f"Exactly two data files need to be provided, {len(mtz_files)} files were given"
            )

        if pdb := self.params["scaleit"].get("pdb"):
            mtz_reindexed = []
            print("HERE")
            # Reindex the mtz files to ensure that they have the same symmetry as the pdb file
            for mtz_file in mtz_files:
                print(mtz_file)
                _filename = os.path.basename(mtz_file)
                _filename = os.path.splitext(_filename)[0]
                hklout = self.working_directory / f"{_filename}_reindexed.mtz"
                pointless_command = [
                    f"pointless hklin {mtz_file} hklout {hklout} xyzin {pdb}"
                ]
                print(pointless_command)
                result = subprocess.run(
                    pointless_command,
                    shell=True,
                    cwd=self.working_directory,
                    capture_output=True,
                    text=True,
                )
                with open(
                    self.working_directory / f"reindex_{_filename}.log", "w"
                ) as log_file:
                    log_file.write(result.stdout)
                with open(
                    self.working_directory / f"reindex_{_filename}_error.log", "w"
                ) as log_file:
                    log_file.write(result.stderr)

                if "Incompatible symmetries" in result.stderr:
                    self.log.error(
                        f"{mtz_file} has incompatible symmetry to ref file {pdb}"
                    )
                    raise Exception(
                        f"{mtz_file} has incompatible symmetry to ref file {pdb}"
                    )
                mtz_reindexed += hklout
            mtz_files = mtz_reindexed

        mtz_nat = mtz_files[0]
        mtz_der = mtz_files[1]

        # Get the F and SIGF data column headings

        # Add the F and SIGF data from one file to the other with cad
        mtz_combi = self.working_directory / "combined.mtz"
        cad_script = [
            f"cad hklin1 {mtz_nat} hklin2 {mtz_der} hklout {mtz_combi} <<END-CAD\n"
            + "TITLE Add data for scaling\n"
            + "LABIN FILE 1 E1=F E2=SIGF\n"
            + "LABIN FILE 2 ALL\n"
            + "LABOUT FILE 1 E1=Fscale E2 = SIGFscale\n"
            + "SYSAB_KEEP\n"
            + "END\n"
            + "END-CAD"
        ]
        result = subprocess.run(
            cad_script,
            shell=True,
            cwd=self.working_directory,
            capture_output=True,
            text=True,
        )
        with open(self.working_directory / "cad.log", "w") as log_file:
            log_file.write(result.stdout)

        # Scale the above data using the data added by cad
        mtz_scaled = self.working_directory / "scaled.mtz"
        scaleit_script = [
            f"scaleit hklin {mtz_combi} hklout {mtz_scaled} <<END-SCALEIT\n"
            + "TITLE Scale data using added ref data\n"
            + "LABIN FP=Fscale SIGFP=SIGFscale FPH1=F SIGFPH1=SIGF DPH1=DANO SIGDPH1=SIGDANO -\n"
            + "FPH1(+)=F(+) SIGFPH1(+)=SIGF(+) FPH1(-)=F(-) SIGFPH1(-)=SIGF(-) IMEAN1=IMEAN -\n"
            + "SIGIMEAN1=SIGIMEAN I1(+)=I(+) SIGI1(+)=SIGI(+) I1(-)=I(-) SIGI1(-)=SIGI(-)\n"
            + "WEIGHT\n"
            + "REFINE SCALE\n"
            + "END\n"
            + "END-SCALEIT"
        ]
        result = subprocess.run(
            scaleit_script,
            shell=True,
            cwd=self.working_directory,
            capture_output=True,
            text=True,
        )
        with open(self.working_directory / "scaleit.log", "w") as log_file:
            log_file.write(result.stdout)

        print("Script finished")
        return True
