from __future__ import annotations

import json
import math
import subprocess

import py

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
        exposure_time = results["exposure_time"]

        d = {
            "ispyb_command": "insert_screening_strategy",
            "transmission": transmission,
            "exposuretime": exposure_time,
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
        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])

        target_saturation = float(params["target_saturation"])
        oscillation = float(params["oscillation"])
        transmission = float(params["transmission"])
        exposure_time = float(params["exposure_time"])

        file = params["input_file"]

        command = [f"xia2.overload {file}"]

        result = subprocess.run(
            command, shell=True, cwd=working_directory, capture_output=True
        )

        if result.returncode:
            self.log.info(f"xia2.overload failed with return code {result.returncode}")
            self.log.debug(f"Command output:\n{result.stdout}")
            return False

        results_directory.ensure(dir=True)
        output_file = "overload.json"
        for file in working_directory.listdir():
            if file.basename != output_file:
                continue

            destination = results_directory.join(file.basename)
            self.log.debug(f"Copying {file.strpath} to {destination.strpath}")
            file.copy(destination)
            self.record_result_individual_file(
                {
                    "file_path": destination.dirname,
                    "file_name": destination.basename,
                    "file_type": file.ext,
                }
            )

        overload_file = working_directory.join(output_file)
        with open(overload_file, "r") as f:
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
        scale_factor = target_saturation / saturation

        scaled_transmission = transmission
        scaled_exposure_time = exposure_time
        if scale_factor < 1:
            scaled_transmission *= scale_factor
            scaled_exposure_time /= scale_factor

        results = {
            "transmission": scaled_transmission,
            "exposure_time": scaled_exposure_time,
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
