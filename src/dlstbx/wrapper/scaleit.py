# Wrapper for method of using ccp4 scaleit to adjust 2 .mtz file to the same overall scale.
from __future__ import annotations

import os
import subprocess
from pathlib import Path

from iotbx import mtz

from dlstbx.wrapper import Wrapper


class ScaleitWrapper(Wrapper):

    _logger_name = "dlstbx.wrap.scaleit"

    def find_cols_from_type(self, obj, type, file):
        """Get the label and corresponding sigma label for a specifed data type from an mtz file.

        Function takes an iotbx mtz.object and mtz column data-type identifier as input and returns
        the corresponding column heading and sigma column heading. Function will only return the
        first column heading of that type.
        """
        col_types = obj.column_types()
        col_labs = obj.column_labels()
        if type in col_types:
            indices = [
                _index for _index, _value in enumerate(col_types) if _value == type
            ]
            if len(indices) > 1:
                self.log.warning(
                    f"Multiple {mtz.column_type_legend[type]} data columns found in {file}, using the first one"
                )
            col_lab = col_labs[indices[0]]
        else:
            self.log.error(
                f"Could not find {mtz.column_type_legend[type]} data column in {file}"
            )
            return None

        if (sig_col_lab := "SIG" + col_lab) not in col_labs:
            self.log.error(f"Could not find {sig_col_lab} data in {file}")
            return None
        return col_lab, sig_col_lab

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
        # Check the input mtz files
        mtz_files = self.params["scaleit"].get("data", [])
        if not mtz_files:
            self.log.error("Could not identify on what data to run")
            return False
        if len(mtz_files) != 2:
            self.log.error(
                f"Exactly two data files need to be provided, {len(mtz_files)} files were given"
            )
            return False
        for _file in mtz_files:
            if not os.path.isfile(_file):
                self.log.error(f"Could not find {_file}")
                return False

        # Reindex the mtz files using pointless to ensure that they have the same symmetry as the pdb file
        if pdb := self.params["scaleit"].get("pdb"):
            mtz_reindexed = []
            for mtz_file in mtz_files:
                print(mtz_file)
                _filename = os.path.basename(mtz_file)
                _filename = os.path.splitext(_filename)[0]
                hklout = self.working_directory / f"{_filename}_reindexed.mtz"
                pointless_command = [
                    f"pointless hklin {mtz_file} hklout {hklout} xyzin {pdb}"
                ]
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
                    return False
                mtz_reindexed.append(hklout)
            mtz_files = mtz_reindexed
        else:
            self.log.info(
                "No pdb file provided, assuming that mtz files have compatible symmetry"
            )

        mtz_nat = mtz_files[0]
        mtz_der = mtz_files[1]

        # Read in mtz files
        obj_nat = mtz.object(str(mtz_nat))
        obj_der = mtz.object(str(mtz_der))

        col_labs = {}
        col_params = [
            ("F_nat", "SIGF_nat", obj_nat, "F", mtz_nat),
            ("F_der", "SIGF_der", obj_der, "F", mtz_der),
            ("DANO_der", "SIGDANO_der", obj_der, "D", mtz_der),
        ]

        for _val, _sigval, obj, type, file in col_params:
            try:
                col_labs[_val], col_labs[_sigval] = self.find_cols_from_type(
                    obj, type, file
                )
            except TypeError:
                self.log.error(f"{_val} and/or {_sigval} missing from {file}")
                return False

        # Add the F and SIGF data from one file to the other with cad
        mtz_combi = self.working_directory / "combined.mtz"
        col_labs_der = (
            obj_der.crystals()[1].datasets()[0].column_labels()
        )  # Get list of column headers excluding hkl
        labin_der = [
            f"E{_i+1}={_label}" for _i, _label in enumerate(col_labs_der)
        ]  # Convert list to cad input format
        cad_script = [
            f"cad hklin1 {mtz_nat} hklin2 {mtz_der} hklout {mtz_combi} <<END-CAD",
            "TITLE Add data for scaling",
            f"LABIN FILE 1 E1={col_labs['F_nat']} E2={col_labs['SIGF_nat']}",
            f"LABIN FILE 2 {' '.join(labin_der)}",
            "LABOUT FILE 1 E1=Fscale E2 = SIGFscale",
            "SYSAB_KEEP",
            "END",
            "END-CAD",
        ]

        cad_script = "\n".join(cad_script)

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
            f"scaleit hklin {mtz_combi} hklout {mtz_scaled} <<END-SCALEIT",
            "TITLE Scale data using added ref data",
            f"LABIN FP=Fscale SIGFP=SIGFscale FPH1={col_labs['F_der']} SIGFPH1={col_labs['SIGF_der']} DPH1={col_labs['DANO_der']} SIGDPH1={col_labs['SIGDANO_der']}",
            "AUTO",
            "WEIGHT",
            "REFINE SCALE",
            "END",
            "END-SCALEIT",
        ]

        scaleit_script = "\n".join(scaleit_script)

        result = subprocess.run(
            scaleit_script,
            shell=True,
            cwd=self.working_directory,
            capture_output=True,
            text=True,
        )
        with open(self.working_directory / "scaleit.log", "w") as log_file:
            log_file.write(result.stdout)
        with open(self.working_directory / "scaleit_error.log", "w") as log_file:
            log_file.write(result.stderr)

        print("Script finished")
        return True
