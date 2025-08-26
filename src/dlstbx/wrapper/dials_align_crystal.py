from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

from dxtbx.model.crystal import CrystalFactory

import dlstbx.util.symlink
from dlstbx.util import ChainMapWithReplacement
from dlstbx.wrapper import Wrapper


class AlignCrystalWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.align_crystal_downstream"

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

            # Step 1: Add new record to Screening table, keep the ScreeningId
            d = {
                "dcid": dcid,
                "programversion": "xia2-dials+dials.align_crystal",
                "comments": settings_str,
                "shortcomments": f"solution {solution_id}",
                "ispyb_command": "insert_screening",
                "store_result": "ispyb_screening_id_%i" % solution_id,
            }
            ispyb_command_list.append(d)

            # Step 2: Store screeningOutput results, linked to the screeningId
            #         Keep the screeningOutputId
            d = {
                "program": "dials.align_crystal",
                "indexingsuccess": 1,
                "strategysuccess": 1,
                "alignmentsuccess": 1,
                "ispyb_command": "insert_screening_output",
                "screening_id": "$ispyb_screening_id_%i" % solution_id,
                "store_result": "ispyb_screening_output_id_%i" % solution_id,
            }
            ispyb_command_list.append(d)

            # Step 3: Store screeningOutputLattice results, linked to the screeningOutputId
            #         Keep the screeningOutputLatticeId
            d = {
                "ispyb_command": "insert_screening_output_lattice",
                "screening_output_id": "$ispyb_screening_output_id_%i" % solution_id,
                "store_result": "ispyb_screening_output_lattice_id_%i" % solution_id,
            }
            uc_params = crystal_symmetry.unit_cell().parameters()
            for i, p in enumerate(("a", "b", "c", "alpha", "beta", "gamma")):
                d["unitcell%s" % p] = uc_params[i]
            d["spacegroup"] = crystal_symmetry.space_group_info().type().lookup_symbol()
            ispyb_command_list.append(d)

            # Step 4: Store screeningStrategy results, linked to the screeningOutputId
            #         Keep the screeningStrategyId
            d = {
                "program": "dials.align_crystal %i" % solution_id,
                "ispyb_command": "insert_screening_strategy",
                "screening_output_id": "$ispyb_screening_output_id_%i" % solution_id,
                "store_result": "ispyb_screening_strategy_id_%i" % solution_id,
            }
            ispyb_command_list.append(d)

            # Step 5: Store screeningStrategyWedge results, linked to the screeningStrategyId
            #         Keep the screeningStrategyWedgeId
            d = {
                "wedgenumber": 1,
                "phi": phi,
                "chi": chi,
                "comments": settings_str,
                "ispyb_command": "insert_screening_strategy_wedge",
                "screening_strategy_id": "$ispyb_screening_strategy_id_%i"
                % solution_id,
                "store_result": "ispyb_screening_strategy_wedge_id_%i" % solution_id,
            }
            ispyb_command_list.append(d)

        if ispyb_command_list:
            self.log.debug("Sending %s", json.dumps(ispyb_command_list, indent=2))
            self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
            self.log.info("Sent %d commands to ISPyB", len(ispyb_command_list))
        else:
            self.log.info("There is no valid dials.align_crystal strategy here")

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        self.params = ChainMapWithReplacement(
            self.recwrap.recipe_step["job_parameters"].get("ispyb_parameters", {}),
            self.recwrap.recipe_step["job_parameters"],
            substitutions=self.recwrap.environment,
        )

        self.working_directory = Path(self.params["working_directory"])
        self.results_directory = Path(self.params["results_directory"])
        self.working_directory.mkdir(parents=True, exist_ok=True)

        symlink = self.params.get("create_symlink")
        if isinstance(symlink, list):
            symlink = symlink[0]
        if symlink:
            dlstbx.util.symlink.create_parent_symlink(self.working_directory, symlink)

        experiment_file = self.params["experiment_file"]
        if isinstance(experiment_file, list):
            experiment_file = experiment_file[0]

        # run dials.align_crystal in working directory
        command = f"dials.align_crystal {experiment_file}"
        self.log.info(f"Running command: {command}")
        result = subprocess.run(
            command,
            shell=True,
            cwd=self.working_directory,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        with open(self.working_directory / "dials.align_crystal.log", "w") as log_file:
            log_file.write(result.stdout)

        if result.returncode:
            self.log.info(
                f"dials.align_crystal failed with return code {result.returncode}"
            )
            self.log.debug(f"Command output:\n{result.stdout}")
            return False

        self.log.info("dials.align_crystal completed successfully")

        self.results_directory.mkdir(parents=True, exist_ok=True)

        if symlink:
            dlstbx.util.symlink.create_parent_symlink(self.results_directory, symlink)

        # copy output files to result directory and attach them in ISPyB

        keep = {"align_crystal.json": "result", "dials.align_crystal.log": "log"}

        for filename in self.working_directory.iterdir():
            filetype = keep.get(filename.name)
            if filetype:
                destination = self.results_directory / filename.name
                self.log.debug(
                    f"Copying {filename.as_posix()} to {destination.as_posix()}"
                )
                shutil.copyfile(filename, destination)
                self.record_result_individual_file(
                    {
                        "file_path": destination.parent,
                        "file_name": destination.name,
                        "file_type": filetype,
                    }
                )
        with open(experiment_file) as fh:
            experiment_data = json.load(fh)
        crystal = CrystalFactory.from_dict(experiment_data["crystal"][0])
        crystal_symmetry = crystal.get_crystal_symmetry()
        # Forward JSON results if possible
        if (self.working_directory / "align_crystal.json").is_file():
            with (self.working_directory / "align_crystal.json").open("rb") as fh:
                json_data = json.load(fh)
            self.insert_dials_align_strategies(
                self.params["dcid"], crystal_symmetry, json_data
            )
        else:
            self.log.warning("Expected JSON output file missing")

        return True
