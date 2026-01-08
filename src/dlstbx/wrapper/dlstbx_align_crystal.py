from __future__ import annotations

import json
import os
import subprocess

import py
from dxtbx.serialize import load

from dlstbx.wrapper import Wrapper


class AlignCrystalWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.dlstbx.align_crystal"

    def insert_dials_align_strategies(self, dcid, crystal_symmetry, results):
        solutions = results["solutions"]
        gonio = results["goniometer"]
        axis_names = gonio["names"]

        kappa_name = None
        chi_name = None
        phi_name = None
        for name in axis_names:
            if "kappa" in name.lower():
                kappa_name = name
            elif "chi" in name.lower():
                chi_name = name
            elif "phi" in name.lower():
                phi_name = name
        assert [chi_name, kappa_name].count(None) == 1
        assert phi_name is not None

        ispyb_command_list = []
        for solution_id, soln in enumerate(solutions):
            if chi_name is not None:
                chi = soln.get(chi_name)
            else:
                chi = None
            if kappa_name is not None:
                kappa = soln.get(kappa_name)
            else:
                kappa = None
            phi = soln.get(phi_name)
            primary_axis = soln.get("primary_axis")[0]
            primary_axis_type = soln.get("primary_axis_type")[0]
            assert [chi, kappa].count(None) == 1
            assert phi is not None
            settings_str = "%s" % primary_axis
            if primary_axis_type is not None:
                settings_str = "%s (%i-fold)" % (settings_str, int(primary_axis_type))

            if kappa is not None and kappa < 0:
                continue  # only insert strategies with positive kappa
            if chi is not None and (chi < 0 or chi > 45):
                continue  # only insert strategies with 0 < chi > 45
            if phi < 0:
                phi += 360  # make phi always positive
            if kappa is not None:
                kappa = "%.2f" % kappa
            elif chi is not None:
                chi = "%.2f" % chi
            phi = "%.2f" % phi

            # Step 1: Store screeningOutput results, linked to the screeningId
            #         Keep the screeningOutputId
            d = {
                "program": "dials.align_crystal",
                "indexingsuccess": 1,
                "strategysuccess": 1,
                "alignmentsuccess": 1,
                "ispyb_command": "insert_screening_output",
                "screening_id": "$ispyb_screening_id",
                "store_result": f"ispyb_screening_output_id_{solution_id}",
            }
            ispyb_command_list.append(d)

            # Step 2: Store screeningOutputLattice results, linked to the screeningOutputId
            #         Keep the screeningOutputLatticeId
            d = {
                "ispyb_command": "insert_screening_output_lattice",
                "screening_output_id": f"$ispyb_screening_output_id_{solution_id}",
                "store_result": f"ispyb_screening_output_lattice_id_{solution_id}",
            }
            uc_params = crystal_symmetry.unit_cell().parameters()
            for i, p in enumerate(("a", "b", "c", "alpha", "beta", "gamma")):
                d["unitcell%s" % p] = uc_params[i]
            d["spacegroup"] = crystal_symmetry.space_group_info().type().lookup_symbol()
            ispyb_command_list.append(d)

            # Step 3: Store screeningStrategy results, linked to the screeningOutputId
            #         Keep the screeningStrategyId
            d = {
                "program": "dials.align_crystal",
                "ispyb_command": "insert_screening_strategy",
                "screening_output_id": f"$ispyb_screening_output_id_{solution_id}",
                "store_result": f"ispyb_screening_strategy_id_{solution_id}",
            }
            ispyb_command_list.append(d)

            # Step 4: Store screeningStrategyWedge results, linked to the screeningStrategyId
            #         Keep the screeningStrategyWedgeId
            d = {
                "wedgenumber": 1,
                "phi": phi,
                "chi": chi,
                "comments": settings_str,
                "ispyb_command": "insert_screening_strategy_wedge",
                "screening_strategy_id": f"$ispyb_screening_strategy_id_{solution_id}",
                "store_result": f"ispyb_screening_strategy_wedge_id_{solution_id}",
            }
            ispyb_command_list.append(d)

        if ispyb_command_list:
            d = {
                "ispyb_command": "update_processing_status",
                "program_id": "$ispyb_autoprocprogram_id",
                "message": "Processing successful",
                "status": "success",
            }
            ispyb_command_list.append(d)

        else:
            d = {
                "ispyb_command": "update_processing_status",
                "program_id": "$ispyb_autoprocprogram_id",
                "message": "No achievable alignment within range of goniometer",
                "status": "failure",
            }
            ispyb_command_list.append(d)
            self.log.info("No achievable alignment within range of goniometer")

        self.log.debug("Sending %s", json.dumps(ispyb_command_list, indent=2))
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        self.log.info("Sent %d commands to ISPyB", len(ispyb_command_list))

    def construct_commandline(self, params):
        """Construct dlstbx.align_crystal command line.
        Takes job parameter dictionary, returns array."""

        pattern = params["image_pattern"]
        first = int(params["image_first"])
        last = int(params["image_last"])
        if pattern.endswith(".cbf"):
            image_files = [
                os.path.join(params["image_directory"], pattern % i)
                for i in range(first, last + 1)
            ]
        else:
            image_files = [os.path.join(params["image_directory"], pattern)]

        command = ["dlstbx.align_crystal"] + image_files

        return command

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        command = self.construct_commandline(params)

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])

        # Create working directory with symbolic link
        working_directory.ensure(dir=True)

        # run dlstbx.align_crystal in working directory
        self.log.info("command: %s", " ".join(command))
        try:
            result = subprocess.run(
                command,
                timeout=params.get("timeout"),
                cwd=working_directory.strpath,
                capture_output=True,
            )
        except subprocess.TimeoutExpired:
            self.log.info(
                f"dlstbx.align_crystal failed by timeout ({params.get('timeout')}s)"
            )
            return False

        if result.returncode:
            self.log.info(
                f"dlstbx.align_crystal failed with returncode {result.returncode}"
            )
            self.log.debug(result.stdout)
            self.log.debug(result.stderr)
            return False

        self.log.info("dlstbx.align_crystal successful")

        # Create results directory and symlink if they don't already exist
        results_directory.ensure(dir=True)

        # copy output files to result directory and attach them in ISPyB
        keep_ext = {
            #'.json': 'result',
            ".log": "log"
        }
        keep = {"align_crystal.json": "result", "dials_symmetry.json": "result"}
        allfiles = []
        for filename in working_directory.listdir():
            filetype = keep_ext.get(filename.ext)
            if filename.basename in keep:
                filetype = keep[filename.basename]
            if filetype is None:
                continue
            destination = results_directory.join(filename.basename)
            self.log.debug(f"Copying {filename.strpath} to {destination.strpath}")
            allfiles.append(destination.strpath)
            filename.copy(destination)
            if filetype:
                self.record_result_individual_file(
                    {
                        "file_path": destination.dirname,
                        "file_name": destination.basename,
                        "file_type": filetype,
                    }
                )
        if allfiles:
            self.record_result_all_files({"filelist": allfiles})

        assert working_directory.join("symmetrized.expt")
        experiments = load.experiment_list(
            working_directory.join("symmetrized.expt").strpath
        )
        crystal_symmetry = experiments[0].crystal.get_crystal_symmetry()
        # Forward JSON results if possible
        if working_directory.join("align_crystal.json").check():
            with working_directory.join("align_crystal.json").open("rb") as fh:
                json_data = json.load(fh)
            self.insert_dials_align_strategies(
                params["dcid"], crystal_symmetry, json_data
            )
        else:
            self.log.warning("Expected JSON output file missing")
            return False

        return True
