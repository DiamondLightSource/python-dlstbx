from __future__ import annotations

import json
import shutil
import subprocess
from collections import Counter
from pathlib import Path
from itertools import accumulate
from dials.array_family import flex

from dlstbx.wrapper import Wrapper

class EstimateTransmissionWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.estimate_transmission"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = Path(params["working_directory"])
        results_directory = Path(params["results_directory"])

        beamline = params["beamline"]
        trusted_countrate_pct = params["trusted_countrate_pct"].get(beamline, 100) / 100
        target_countrate_pct = params["target_max_pixel_countrate_pct"].get(beamline, 50) / 100
        transmission = float(params["transmission"])
        file = params["input_file"]

        commands = [
            ("dials.import", ["dials.import", file]),
            ("dials.find_spots", [ "dials.find_spots", 
                                  "imported.expt", 
                                  "ice_rings.filter=True"],
            ),
        ]

        for command, script in commands:
            result = subprocess.run(script, cwd=working_directory,  check=True)

            if result.returncode:
                self.log.info(f"{command} failed with return code {result.returncode}")
                self.log.info(result.stderr)

                self.log.debug(f"Command output:\n{result.stdout}")
                self.log.debug(f"From command: {script}")
                return False

        experiment_file = results_directory / "imported.expt"
        with experiment_file.open("r") as f:
            experiment = json.load(f)
            trusted_range = experiment["detector"][0]["panels"][0]["trusted_range"][1]

        reflection_file = results_directory / "strong.refl"
        reflections = flex.reflection_table.from_file(reflection_file)
        counts_hist = self.build_hist_from_reflections(reflections)
        self.save_hist_to_json(counts_hist, trusted_range, results_directory)
        
        results_directory.mkdir(parents=True, exist_ok=True)
        output_files = ["dials.find_spots.log"]

        for output_file in output_files:
            source_file = working_directory / output_file
            destination = results_directory / output_file

            if not source_file.exists():
                self.log.info(f"{source_file=} does not exsist")
                return False

            self.log.info(f"Copying {str(source_file)} to {str(destination)}")
            shutil.copy(source_file, destination)


        num_counts, num_spots = counts_hist
        max_counts = num_counts[-1]  

        max_pixel_countrate_pct = max_counts / trusted_range 
        if max_pixel_countrate_pct < trusted_countrate_pct: 
            self.log.info(f"Max pixel is less intense than the trusted value of {trusted_countrate_pct}. Pixel intensity is {max_pixel_countrate_pct}")
            self.log.info(f"Recommended transmission will not be scaled")

            self.recwrap.send_to(
                "strategy",
                {"parameters": {"scaled_transmission": transmission}},
            )

            self.log.info("Done.")
            return True

        scale_factor =  target_countrate_pct / max_pixel_countrate_pct

        scaled_transmission = min(1, (transmission * scale_factor) / 100)
        self.log.info(f"Scaled transmission is : {scaled_transmission}")

        self.recwrap.send_to(
            "strategy",
            {"parameters": {"scaled_transmission": float(scaled_transmission)}},
        )

        self.log.info("Done.")
        return True

    def build_hist_from_reflections(self, reflections):
        "Iterate through the shoeboxes to a reflection and generate a pixel histogram"

        shoeboxes = reflections["shoebox"]
        counter = Counter()
        for sbox in shoeboxes:
            counter.update(sbox.data.as_numpy_array().ravel())

        sorted_counter = sorted(counter.items())
        return zip(*sorted_counter)

    def save_hist_to_json(self, hist, max_trusted_value, results_dir):
        counts_pixel_data = dict(hist)

        results_path = results_dir / "overload.json"
        self.log.info("Saving counts histogram to", results_path)
        with open(results_path, 'w') as f:
            json.dump({ "counts": counts_pixel_data,
                        "overload_limit": max_trusted_value}, f)
        
        self.log.info("Saved.")
