# Wrapper for method of using ccp4 scaleit to adjust 2 .mtz file to the same overall scale.
from __future__ import annotations

import subprocess
from pathlib import Path

from dlstbx.wrapper import Wrapper


class ScaleitWrapper(Wrapper):

    _logger_name = "dlstbx.wrap.scaleit"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.debug(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        # Get parameters from the recipe file
        self.params = self.recwrap.recipe_step["job_parameters"]
        # Timestamp for creating unique file output for prototyping - remove when recipe file is sorted.
        # timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        # self.working_directory = (
        #     Path(self.params["working_directory"]) / timestamp
        # )  # Timestamp added for prototyping to create unique directory
        # self.results_directory = (
        #     Path(self.params["results_directory"]) / timestamp
        # )  # Timestamp added for prototyping to create unique directory
        self.working_directory = Path(
            self.recwrap.recipe_step["parameters"]["workingdir"]
        )
        # Make the directories if they don't already exist
        self.working_directory.mkdir(parents=True, exist_ok=True)
        self.log.info(f"Here are the params {self.params}")
        mtz = self.params["scaleit"].get("data", [])
        if not mtz:
            self.log.error("Could not identify on what data to run")
            return False
        if len(mtz) != 2:
            self.log.error(
                f"Exactly two data files need to be provided, {len(mtz)} files were given"
            )

        mtz_nat = mtz[0]
        mtz_der = mtz[1]

        # Add the F and SIGF data from one file to the other with cad
        mtz_combi = self.working_directory / "combined.mtz"
        cad_script = [
            f"cad hklin1 {mtz_nat} hklin2 {mtz_der} hklout {mtz_combi} <<END-CAD\n"
            + "TITLE Add data for scaling\n"
            + "LABIN FILE 1 E1=F E2=SIGF\n"
            + "LABIN FILE 2 -\n"
            + "E1=FreeR_flag E2=IMEAN E3=SIGIMEAN E4=N E5=I(+) E6=SIGI(+) E7=I(-) -\n"
            + "E8=SIGI(-) E9=N(+) E10=N(-) E11=F E12=SIGF E13=F(+) E14=SIGF(+) -\n"
            + "E15=F(-) E16=SIGF(-) E17=DANO E18=SIGDANO\n"
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
