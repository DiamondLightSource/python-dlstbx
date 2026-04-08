from __future__ import annotations

import json
import math
import subprocess

from pathlib import Path
import shutil

from dlstbx.wrapper import Wrapper


class Xia2OverloadWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.xia2_overload"

    def send_to_ispyb(self, results):
        ispyb_command_list = []

        d = {
            "program": "xia2.overload",
            "ispyb_command": "insert_screening_output",
            "screening_id": "$ispyb_screening_id",
            "store_result": "ispyb_screening_output_id",
        }
        ispyb_command_list.append(d)
        transmission = results["transmission"]

        d = {
            "ispyb_command": "insert_screening_strategy",
            "transmission": transmission,
            "screening_output_id": "$ispyb_screening_output_id",
            "store_result": "ispyb_screening_strategy_id",
        }
        ispyb_command_list.append(d)

        d = {
            "ispyb_command": "update_processing_status",
            "program_id": "$ispyb_autoprocprogram_id",
            "message": "Processing successful",
            "status": "success",
        }
        ispyb_command_list.append(d)

        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        self.log.info("Sent %d commands to ISPyB", len(ispyb_command_list))
        self.log.debug("Sending %s", json.dumps(ispyb_command_list, indent=2))

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = Path(params["working_directory"])
        results_directory = Path(params["results_directory"])

        target_countrate_pct = float(params["target_countrate_pct"])
        oscillation = float(params["oscillation"])
        transmission = float(params["transmission"])

        file = params["input_file"]

        command = [f"xia2.overload {file}"]

        result = subprocess.run(
            command, shell=True, cwd=working_directory, capture_output=True
        )

        if result.returncode:
            self.log.info(f"xia2.overload failed with return code {result.returncode}")
            self.log.info(result.stderr)
            self.log.debug(f"Command output:\n{result.stdout}")
            return False

        results_directory.mkdir(parents=True, exist_ok=True)
        output_file_name = "overload.json"
        
        source_file = working_directory / output_file_name
        destination = results_directory / output_file_name

        if not source_file.exists():
            return False
        
        self.log.debug(f"Copying {str(source_file)} to {str(destination)}")
        shutil.copy2(source_file, destination)
        
        self.record_result_individual_file(
            {
                "file_path": str(destination.parent),
                "file_name": destination.name,
                "file_type": "result",
            }
        )

        with source_file.open("r") as f:
            data = json.load(f)
            counts = data["counts"]
            overload_limit = float(data["overload_limit"])

        max_count = float(list(counts)[-1])

        mosaicity_corr = params.get("mosaicity_correction", False)
        average_to_peak = (
            self.mosaicity_correction(mosaicity_corr, oscillation)
            if mosaicity_corr
            else 1
        )

        saturation = (max_count / overload_limit) * average_to_peak
        scale_factor = target_countrate_pct / saturation

        scaled_transmission = transmission
        if scale_factor < 1:
            scaled_transmission *= scale_factor

        results = {
            "transmission": scaled_transmission,
        }

        self.send_to_ispyb(results)

        self.log.info("Done.")
        return True

    def mosaicity_correction(self, moscaicity_coefficent: float, oscillation: float):
        delta_z = oscillation / (moscaicity_coefficent) * math.sqrt(2)
        average_to_peak = (
            math.sqrt(math.pi) * delta_z * math.erf(delta_z)
            + math.exp(-(delta_z * delta_z))
            - 1
        ) / (delta_z * delta_z)
        self.log.info("Average-to-peak intensity ratio: %f", average_to_peak)
        return average_to_peak
