from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path

import dlstbx.util.symlink
from dlstbx.wrapper import Wrapper


class MosflmStrategyWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.mosflm_strategy"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = Path(params["working_directory"])
        results_directory = Path(params["results_directory"])
        self.log.info("working_directory: %s", working_directory)
        working_directory.mkdir(parents=True, exist_ok=True)
        results_directory.mkdir(parents=True, exist_ok=True)

        symlink = params.get("create_symlink", "strategy-mosflm")
        if isinstance(symlink, list):
            symlink = symlink[0]
        if symlink:
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(working_directory), symlink
            )
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(results_directory), symlink
            )

        # Set SynchWeb to swirl
        (results_directory / "strategy_native.log").touch()

        if params["image_pattern"].endswith(".h5"):
            if not self.hdf5_to_cbf():
                return False

        image_directory = params["image_directory"]
        image_pattern = params["image_pattern"]
        image_first = int(params["image_first"])
        image_file_name = str(Path(image_directory) / (image_pattern % image_first))
        commands = ["som.strategy", image_file_name]
        space_group = params.get("spacegroup")
        if space_group:
            commands.append(space_group)
        self.log.info("command: %s", " ".join(commands))
        exitcode, timed_out, runtime, stdout, stderr = self._run_command(
            commands,
            working_directory,
            params.get("timeout", 600),
        )
        success = exitcode == 0 and not timed_out
        if success:
            self.log.info("som.strategy successful, took %.1f seconds", runtime)
        else:
            self.log.info(
                "som.strategy failed with exitcode %s and timeout %s",
                exitcode,
                timed_out,
            )
            self.log.debug(stdout)
            self.log.debug(stderr)

        if (working_directory / "mosflm_index.mat").exists():
            self.recwrap.send_to(
                "indexing-solution",
                {"index.mat": str(working_directory / "mosflm_index.mat")},
            )
        if (
            not (working_directory / "strategy_native.log").exists()
            or (working_directory / "strategy_native.log").stat().st_size == 0
        ):
            success = False
            (working_directory / "strategy_native.log").write_text(
                "failed to determine strategy"
            )

        strategy_dat = working_directory / "strategy.dat"
        if strategy_dat.exists():
            results = self.parse_strategy_dat(strategy_dat)
            self.recwrap.send_to("strategy", results)
            # insert results into database
            self.send_screening_result_to_ispyb(params["dcid"], results)

        # copy output files to result directory
        self.log.info(
            "Copying results from %s to %s",
            working_directory,
            results_directory,
        )
        for output_file in working_directory.iterdir():
            if output_file.name.startswith("."):
                continue
            destination = results_directory / output_file.name
            if output_file.is_dir():
                shutil.copytree(output_file, destination, dirs_exist_ok=True)
            else:
                shutil.copy2(output_file, destination)

        return success

    def parse_strategy_dat(self, strategy_dat):
        lines = strategy_dat.read_text().splitlines()
        tokens = [line.strip().split(",") for line in lines]
        self.log.debug(tokens)

        return {
            "unit_cell": tokens[0][1:7],
            "space_group": tokens[0][7],
            "mosaicity": tokens[0][8],
            "strategy_native": {
                "anomalous": False,
                "comments": "MOSFLM native",
                "axisstart": tokens[1][1],
                "axisend": tokens[1][2],
                "oscillationrange": tokens[1][3],
                "noimages": tokens[1][4],
                "completeness": tokens[1][5],
                "resolution": tokens[1][6],
            },
            "strategy_anomalous": {
                "anomalous": True,
                "comments": "MOSFLM anomalous",
                "axisstart": tokens[2][1],
                "axisend": tokens[2][2],
                "oscillationrange": tokens[2][3],
                "noimages": tokens[2][4],
                "completeness": tokens[2][5],
                "resolution": tokens[2][6],
            },
        }

    def hdf5_to_cbf(self):
        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = Path(params["working_directory"])
        if params.get("temporary_directory"):
            tmpdir = Path(params["temporary_directory"])
        else:
            tmpdir = working_directory / ".image-tmp"
        tmpdir.mkdir(parents=True, exist_ok=True)
        master_h5 = Path(params["image_directory"]) / params["image_pattern"]
        prefix = params["image_pattern"].split("master.h5")[0]
        params["image_pattern"] = prefix + "%04d.cbf"
        self.log.info("Image pattern: %s", params["image_pattern"])
        self.log.info(
            "Converting %s to %s", master_h5, tmpdir / params["image_pattern"]
        )
        exitcode, timed_out, runtime, stdout, stderr = self._run_command(
            ["dxtbx.dlsnxs2cbf", str(master_h5), "-t", prefix, "-n 4"],
            tmpdir,
            params.get("timeout", 600),
        )
        success = exitcode == 0 and not timed_out
        if success:
            self.log.info("dxtbx.dlsnxs2cbf successful, took %.1f seconds", runtime)
        else:
            self.log.error(
                "dxtbx.dlsnxs2cbf failed with exitcode %s and timeout %s",
                exitcode,
                timed_out,
            )
            self.log.debug(stdout)
            self.log.debug(stderr)
        params["orig_image_directory"] = params["image_directory"]
        params["image_directory"] = str(tmpdir)
        return success

    def _run_command(self, command, working_directory, timeout):
        start_time = time.monotonic()
        try:
            result = subprocess.run(
                command,
                cwd=working_directory,
                timeout=timeout,
                capture_output=True,
                text=True,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            stdout = error.stdout or ""
            stderr = error.stderr or ""
            if isinstance(stdout, bytes):
                stdout = stdout.decode(errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode(errors="replace")
            return (
                None,
                True,
                time.monotonic() - start_time,
                stdout,
                stderr,
            )
        return (
            result.returncode,
            False,
            time.monotonic() - start_time,
            result.stdout,
            result.stderr,
        )

    def send_screening_result_to_ispyb(self, dcid, results):
        # example strategy.dat output:
        #
        # character,57.910,57.910,149.806,90.000,90.000,90.000,P4,0.55
        # mosflm native,182.0,272.0,1.2,75,1.00,1.47
        # mosflm anomalous,182.0,272.0,1.2,75,0.92,1.47

        # assert dcid > 0, "Invalid data collection ID given."

        ispyb_command_list = []

        for i, strategy in enumerate(
            [results["strategy_native"], results["strategy_anomalous"]]
        ):
            # Step 1: Store screeningOutput results, linked to the screeningId
            #         Keep the screeningOutputId
            d = {
                "program": "mosflm",
                "indexingsuccess": 1,
                "strategysuccess": 1,
                "ispyb_command": "insert_screening_output",
                "screening_id": "$ispyb_screening_id",
                "store_result": "ispyb_screening_output_id_%i" % i,
            }
            ispyb_command_list.append(d)

            # Step 2: Store screeningOutputLattice results, linked to the screeningOutputId
            #         Keep the screeningOutputLatticeId
            d = {
                "spacegroup": results["space_group"],
                "unitcella": results["unit_cell"][0],
                "unitcellb": results["unit_cell"][1],
                "unitcellc": results["unit_cell"][2],
                "unitcellalpha": results["unit_cell"][3],
                "unitcellbeta": results["unit_cell"][4],
                "unitcellgamma": results["unit_cell"][5],
                "ispyb_command": "insert_screening_output_lattice",
                "screening_output_id": "$ispyb_screening_output_id_%i" % i,
                "store_result": "ispyb_screening_output_lattice_id_%i" % i,
            }
            ispyb_command_list.append(d)

            # Step 3: Store screeningStrategy results, linked to the screeningOutputId
            #         Keep the screeningStrategyId
            d = {
                "program": "mosflm",
                "anomalous": strategy["anomalous"],
                "ispyb_command": "insert_screening_strategy",
                "screening_output_id": "$ispyb_screening_output_id_%i" % i,
                "store_result": "ispyb_screening_strategy_id_%i" % i,
            }
            ispyb_command_list.append(d)

            # Step 4: Store screeningStrategyWedge results, linked to the screeningStrategyId
            #         Keep the screeningStrategyWedgeId
            d = {
                "wedgenumber": 1,
                "resolution": strategy["resolution"],
                "completeness": strategy["completeness"],
                "noimages": strategy["noimages"],
                "comments": strategy["comments"],
                "ispyb_command": "insert_screening_strategy_wedge",
                "screening_strategy_id": "$ispyb_screening_strategy_id_%i" % i,
                "store_result": "ispyb_screening_strategy_wedge_id_%i" % i,
            }
            ispyb_command_list.append(d)

            # Step 5: Store screeningStrategySubWedge results, linked to the screeningStrategyWedgeId
            #         Keep the screeningStrategySubWedgeId
            d = {
                "subwedgenumber": 1,
                "rotationaxis": "omega",
                "comments": strategy["comments"],
                "ispyb_command": "insert_screening_strategy_sub_wedge",
                "screening_strategy_wedge_id": "$ispyb_screening_strategy_wedge_id_%i"
                % i,
                "store_result": "ispyb_screening_strategy_sub_wedge_id_%i" % i,
            }
            for k in (
                "resolution",
                "axisstart",
                "axisend",
                "oscillationrange",
                "noimages",
                "completeness",
            ):
                d[k] = strategy[k]
            ispyb_command_list.append(d)

        self.log.info("Sending %s", json.dumps(ispyb_command_list, indent=2))
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        self.log.info("Sent %d commands to ISPyB", len(ispyb_command_list))
