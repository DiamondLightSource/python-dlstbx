# Wrapper for method of using ccp4 scaleit to adjust 2 mtz file to the same overall scale.
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from iotbx import mtz

from dlstbx.wrapper import Wrapper


class ScaleitWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.scaleit"

    def find_cols_from_type(self, obj, type, file="mtz_file"):
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

    def calc_amplitudes(self, mtz_obj, mtz_file):
        """
        Use truncate to calculate amplitudes from IMEAN data.

        Takes mtz object and mtz file name and runs truncate to calculate
        amplitudes. Returns a new mtz_object and file name for the output file
        with suffix: "_amplit.mtz"
        """
        if "F" not in mtz_obj.column_types():
            _col_lab, _sig_col_lab = self.find_cols_from_type(mtz_obj, "J", mtz_file)
            self.log.info(
                f"Amplitude data not in {mtz_file}, running TRUNCATE to calculate"
            )
            file_basename = os.path.splitext(os.path.basename(mtz_file))[0]
            amplit_file_name = f"{file_basename}_amplit.mtz"
            amplit_file = self.working_directory / amplit_file_name
            truncate_script = [
                f"truncate hklin {mtz_file} hklout {amplit_file} <<END-TRUNCATE",
                f"labin IMEAN={_col_lab} SIGIMEAN={_sig_col_lab}",
                "labout F=F SIGF=SIGF",
                "NOHARVEST",
                "END",
                "END-TRUNCATE",
            ]
            _, _ = self.ccp4_command(truncate_script, f"{file_basename}_truncate")

            amplit_obj = mtz.object(str(amplit_file))
            return amplit_obj, amplit_file
        else:
            return mtz_obj, mtz_file

    def ccp4_command(self, _script, _output):
        _command = "\n".join(_script)

        result = subprocess.run(
            _command,
            shell=True,
            cwd=self.working_directory,
            capture_output=True,
            text=True,
        )
        with open(self.working_directory / f"{_output}.log", "w") as _log_file:
            _log_file.write(result.stdout)
        with open(self.working_directory / f"{_output}_error.log", "w") as _log_file:
            _log_file.write(result.stderr)
        return result.stdout, result.stderr

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.debug(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        # Get parameters from the recipe file
        self.params = self.recwrap.recipe_step["job_parameters"]

        # Get the working and results directories
        self.working_directory = Path(self.params["working_directory"])
        self.results_directory = Path(self.params["results_directory"])
        # Make the directories if they don't already exist
        self.working_directory.mkdir(parents=True, exist_ok=True)
        self.results_directory.mkdir(parents=True, exist_ok=True)
        # Check the input mtz files
        src_mtz_files = self.params["scaleit"].get("data", [])
        if not src_mtz_files:
            self.log.error("Could not identify on what data to run")
            return False
        if len(src_mtz_files) != 2:
            self.log.error(
                f"Exactly two data files need to be provided, {len(src_mtz_files)} files were given"
            )
            return False
        # Copy the source mtz_files files to the working directory
        mtz_files = []
        for _file in src_mtz_files:
            _file_name = os.path.basename(_file)
            _dest_file = self.working_directory / _file_name
            # If input mtz files have the same file name (e.g. fast_dp.mtz), add number to differentiate files
            if _dest_file in mtz_files:
                _dest_file = _dest_file.with_name(
                    f"{_dest_file.stem}_{len(mtz_files)}{_dest_file.suffix}"
                )
            try:
                shutil.copy(_file, _dest_file)
                self.log.info(f"File '{_file}' copied to '{_dest_file}'")
                mtz_files.append(_dest_file)
            except FileNotFoundError:
                self.log.error(f"Source file '{_file}' not found.")
                return False
            except PermissionError:
                self.log.error(
                    f"Permission denied for copying '{_file}' to '{_dest_file}'."
                )
                return False

        mtz_nat = self.working_directory / mtz_files[0]
        mtz_der = self.working_directory / mtz_files[1]

        # Ensure that the mtz files have compatible symmetry and put them into the same space group using pointless
        mtz_der_filename = os.path.splitext(os.path.basename(mtz_der))[0]
        hklout = self.working_directory / f"{mtz_der_filename}_reindexed.mtz"
        pointless_command = [
            f"pointless hklin {mtz_der} hklout {hklout} hklref {mtz_nat}"
        ]
        _, pointless_error = self.ccp4_command(
            pointless_command, f"reindex_{mtz_der_filename}"
        )

        if "Incompatible symmetries" in pointless_error:
            self.log.error("Scaleit - mtz files have incompatible symmetry")
            return False
        # Update mtz_der to the reindexed file path
        mtz_der = hklout

        # Read in mtz files
        obj_nat = mtz.object(str(mtz_nat))
        obj_der = mtz.object(str(mtz_der))

        # Calculate structure factors if needed using truncate
        obj_nat, mtz_nat = self.calc_amplitudes(obj_nat, mtz_nat)
        obj_der, mtz_der = self.calc_amplitudes(obj_der, mtz_der)

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
        mtz_combi = self.working_directory / f"{mtz_der_filename}_combined.mtz"
        # Get list of column headers excluding hkl
        col_labs_der = obj_der.crystals()[1].datasets()[0].column_labels()
        # Convert list to cad input format
        labin_der = [f"E{_i+1}={_label}" for _i, _label in enumerate(col_labs_der)]
        cad_script = [
            f"cad hklin1 {mtz_der} hklin2 {mtz_nat} hklout {mtz_combi} <<END-CAD",
            "TITLE Add data for scaling",
            f"LABIN FILE 2 E1={col_labs['F_nat']} E2={col_labs['SIGF_nat']}",
            f"LABIN FILE 1 {' '.join(labin_der)}",
            "LABOUT FILE 2 E1=Fscale E2 = SIGFscale",
            "SYSAB_KEEP",
            "END",
            "END-CAD",
        ]

        self.ccp4_command(cad_script, "cad")

        # Scale the above data using the data added by cad
        mtz_combined_scaled = (
            self.working_directory / f"{mtz_der_filename}_combined_scaled.mtz"
        )
        scaleit_script = [
            f"scaleit hklin {mtz_combi} hklout {mtz_combined_scaled} <<END-SCALEIT",
            "TITLE Scale data using added ref data",
            f"LABIN FP=Fscale SIGFP=SIGFscale FPH1={col_labs['F_der']} SIGFPH1={col_labs['SIGF_der']} DPH1={col_labs['DANO_der']} SIGDPH1={col_labs['SIGDANO_der']}",
            "AUTO",
            "WEIGHT",
            "REFINE SCALE",
            "END",
            "END-SCALEIT",
        ]

        self.ccp4_command(scaleit_script, "scaleit")

        # Remove the reference columns used for scaling from the mtz file using mtzutils
        self.log.info(f"Removing scaling columns from {mtz_combined_scaled}")
        mtz_scaled = self.working_directory / f"{mtz_der_filename}_scaled.mtz"
        mtzutil_script = [
            f"mtzutils hklin {mtz_combined_scaled} hklout {mtz_scaled} <<END-MTZUTILS",
            "EXCLUDE Fscale SIGFscale",
            "END",
            "END-MTZUTILS",
        ]

        self.ccp4_command(mtzutil_script, "mtzutil")

        # Convert output files to specified output names if given
        files_out = self.params["scaleit"].get("files_out", [])
        if files_out:
            if len(files_out) != len(src_mtz_files):
                self.log.error(
                    "Files_out list has different length to source mtz_files list"
                )
                return False
            shutil.copy(mtz_nat, self.working_directory / files_out[0])
            shutil.copy(mtz_scaled, self.working_directory / files_out[1])

        self.log.info(f"Copying Scaleit results to {self.results_directory}")
        for f in self.working_directory.iterdir():
            if f.name.startswith("."):
                continue
            shutil.copy(f, self.results_directory)

        return True
