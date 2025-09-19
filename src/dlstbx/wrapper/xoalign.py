from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import dlstbx.util.symlink
from dlstbx.util import ChainMapWithReplacement
from dlstbx.wrapper import Wrapper


class XOalignWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.xoalign"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = ChainMapWithReplacement(
            self.recwrap.recipe_step["job_parameters"].get("ispyb_parameters", {}),
            self.recwrap.recipe_step["job_parameters"],
            substitutions=self.recwrap.environment,
        )

        beamline = params["beamline"]
        if beamline not in ("i03", "i04"):
            # Only run XOalign on these beamlines
            return True

        if isinstance(params["experiment_file"], list):
            experiment_file = Path(params["experiment_file"][0])
        else:
            experiment_file = Path(params.get("experiment_file"))

        if not experiment_file.is_file():
            self.log.error(
                f"Input file {experiment_file} does not exist, could not run xoalign"
            )
            return False

        working_directory = Path(params["working_directory"])
        results_directory = Path(params["results_directory"])

        # Create working directory
        working_directory.mkdir(parents=True, exist_ok=True)

        symlink = self.params.get("create_symlink")
        if isinstance(symlink, list):
            symlink = symlink[0]
        if symlink:
            dlstbx.util.symlink.create_parent_symlink(working_directory, symlink)

        chi = params.get("chi")
        kappa = params.get("kappa")
        omega = params.get("omega")
        phi = params.get("phi")
        if kappa != "None":
            datum = f"-D {phi},{kappa},{omega}"
        elif chi != "None":
            datum = f"-D {phi},{chi},{omega}"
        else:
            datum = ""

        subprocess_environment = os.environ.copy()
        subprocess_environment["XOALIGN_CALIB"] = (
            f"/dls_sw/{beamline}/etc/xoalign_config.py"
        )

        commands = [
            "XOalign.py",
            *([datum] if datum else []),
            experiment_file.as_posix(),
        ]

        self.log.info("command: %s", " ".join(commands))
        result = subprocess.run(
            commands,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=subprocess_environment,
            cwd=working_directory,
            text=True,
        )
        success = not result.returncode
        if success:
            self.log.info("XOalign successful")
        else:
            self.log.info(
                f"XOalign failed with returncode {result.returncode}",
            )
            self.log.debug(result.stdout)

        with open(working_directory / "XOalign.log", "w") as log_file:
            log_file.write(result.stdout)

        self.insertXOalignStrategies(params["dcid"], result.stdout)

        results_directory.mkdir(parents=True, exist_ok=True)

        if symlink:
            dlstbx.util.symlink.create_parent_symlink(results_directory, symlink)

        # copy output files to result directory
        self.log.info(
            f"Copying results from {working_directory} to {results_directory}"
        )
        for path in working_directory.iterdir():
            if path.is_file() and not path.name.startswith("."):
                shutil.copy(path, results_directory)
        return success

    def insertXOalignStrategies(self, dcid, xoalign_log):
        smargon = False
        found_solutions = False

        ispyb_command_list = []
        for line in xoalign_log.splitlines(True):
            if "Independent Solutions" in line:
                found_solutions = True
                if "SmarGon" in line:
                    smargon = True
                continue

            if not found_solutions:
                continue

            kappa = None
            chi = None
            phi = None
            tokens = line.split()
            if len(tokens) < 4:
                continue

            solution_id = int(tokens[0])
            angles = [float(t) for t in tokens[1:3]]
            if smargon:
                chi, phi = angles
            else:
                kappa, phi = angles
            settings_str = " ".join(tokens[3:]).replace("'", "")

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
                "programversion": "XOalign",
                "comments": settings_str,
                "shortcomments": "XOalign %i" % solution_id,
                "ispyb_command": "insert_screening",
                "store_result": "ispyb_screening_id_%i" % solution_id,
            }
            ispyb_command_list.append(d)

            # Step 2: Store screeningOutput results, linked to the screeningId
            #         Keep the screeningOutputId
            d = {
                "program": "XOalign",
                "indexingsuccess": 1,
                "strategysuccess": 1,
                "alignmentsuccess": 1,
                "ispyb_command": "insert_screening_output",
                "screening_id": "$ispyb_screening_id_%i" % solution_id,
                "store_result": "ispyb_screening_output_id_%i" % solution_id,
            }
            ispyb_command_list.append(d)

            # Step 3: Store screeningStrategy results, linked to the screeningOutputId
            #         Keep the screeningStrategyId
            d = {
                "program": "XOalign",
                "ispyb_command": "insert_screening_strategy",
                "screening_output_id": "$ispyb_screening_output_id_%i" % solution_id,
                "store_result": "ispyb_screening_strategy_id_%i" % solution_id,
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
            self.log.info("There is no valid XOalign strategy here")
