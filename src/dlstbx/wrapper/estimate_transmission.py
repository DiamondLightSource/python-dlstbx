from __future__ import annotations
from dials.array_family import flex

import math
import subprocess
import json
from pathlib import Path
import shutil
from collections import Counter
from itertools import accumulate

from dlstbx.wrapper import Wrapper

def build_hist_from_reflections(reflections):
    "Iterate through the shoeboxes to a reflection and generate a pixel histogram"

    shoeboxes = reflections["shoebox"]
    counter = Counter()
    for sbox in shoeboxes:
        counter.update(sbox.data.as_numpy_array().ravel())

    sorted_counter = sorted(counter.items()) 
    return zip(*sorted_counter)

def get_percentile_index(num_pixels, percentile):
    threshold = sum(num_pixels) * percentile
    
    for i, cum_sum in enumerate(accumulate(num_pixels)):
        if cum_sum >= threshold:
            return i
            
    return len(num_pixels)
    
    
class EstimateTransmissionWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.estimate_transmission"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = Path(params["working_directory"])
        results_directory = Path(params["results_directory"])

        beamline = params["beamline"]
        pixel_percentile = params["pixel_percentile"].get(beamline, 100) / 100 
        target_countrate_pct = params["target_countrate_pct"].get(beamline, 25) / 100
        oscillation = float(params["oscillation"])
        transmission = float(params["transmission"])
        file = params["input_file"]

        commands = {
            "dials.import": ["dials.import", file],
            "dials.find_spots": ["dials.find_spots", "imported.expt", "ice_rings.filter=True"],
        }
        
        for command, script in commands.items():
            result = subprocess.Popen(script, cwd=working_directory)
            result.wait() 
            if result.returncode:
                self.log.info(f"{command} failed with return code {result.returncode}")
                self.log.info(result.stderr)
                
                self.log.debug(f"Command output:\n{result.stdout}")
                self.log.debug(f"From command: {script}")
                return False

        results_directory.mkdir(parents=True, exist_ok=True)
        output_files = ["dials.find_spots.log", "strong.refl", "imported.expt"]
        
        # Copy files to  
        for output_file in output_files:
            source_file = working_directory / output_file
            destination = results_directory / output_file

            if not source_file.exists():
                self.log.info(f"{source_file=} does not exsist")
                return False

            self.log.info(f"Copying {str(source_file)} to {str(destination)}")
            shutil.copy(source_file, destination)
        
        experiment_file = results_directory / "imported.expt"
        with experiment_file.open("r") as f:
            experiment = json.load(f)
            trusted_range = experiment["detector"][0]["panels"][0]["trusted_range"][1]

        reflection_file = results_directory / "strong.refl"
        reflections = flex.reflection_table.from_file(reflection_file) 
        num_counts, num_pixels = build_hist_from_reflections(reflections)

        index_of_percentile = get_percentile_index(num_pixels, pixel_percentile)
        counts_at_percentile = num_counts[index_of_percentile]
        
        mosaicity_corr = params.get("mosaicity_correction", False)
        average_to_peak = (
                    self.mosaicity_correction(mosaicity_corr, oscillation)
                    if mosaicity_corr
                    else 1
        )

        countrate_saturation = counts_at_percentile / trusted_range
        self.log.info(f"Countrate saturation is : {counts_at_percentile / trusted_range}")
        scale_factor = average_to_peak * target_countrate_pct / countrate_saturation

        scaled_transmission = min(1, (transmission * scale_factor) / 100)
        self.log.info(f"Scaled transmission is : {scaled_transmission}")

        self.recwrap.send_to("strategy", {"parameters": {"scaled_transmission": float(scaled_transmission)}})
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
