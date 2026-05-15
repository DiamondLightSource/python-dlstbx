from __future__ import annotations

import json
import math
import shutil
import subprocess
from collections import Counter
from itertools import accumulate
from pathlib import Path

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
        pixel_percentile = params["pixel_percentile"].get(beamline, 100) / 100
        target_countrate_pct = params["target_countrate_pct"].get(beamline, 50) / 100
        transmission = float(params["transmission"])
        file = params["input_file"]

        commands = [
            ("dials.import", ["dials.import", file]),
            (
                "dials.find_spots",
                ["dials.find_spots", "imported.expt", "ice_rings.filter=True"],
            ),
        ]

        for command, script in commands:
            result = subprocess.run(script, cwd=working_directory, check=True)

            if result.returncode:
                self.log.info(f"{command} failed with return code {result.returncode}")
                self.log.info(result.stderr)

                self.log.debug(f"Command output:\n{result.stdout}")
                self.log.debug(f"From command: {script}")
                return False

        experiment_file = working_directory / "imported.expt"
        with experiment_file.open("r") as f:
            experiment = json.load(f)
            trusted_range = experiment["detector"][0]["panels"][0]["trusted_range"][1]

        reflection_file = working_directory / "strong.refl"
        reflections = flex.reflection_table.from_file(reflection_file)
        counts_hist = self.collect_counts_from_reflections(reflections)

        num_counts = list(counts_hist.keys())
        num_pixels = list(counts_hist.values())

        index_of_pixel_percentile = self.get_percentile_index(
            num_pixels, pixel_percentile
        )
        counts_at_percentile = int(num_counts[index_of_pixel_percentile])

        pixel_countrate_pct = counts_at_percentile / trusted_range
        self.log.info(
            f"The countrate percentage of the {pixel_percentile * 100}% most intense pixel is {pixel_countrate_pct * 100}% of the trusted value"
        )
        scale_factor = target_countrate_pct / pixel_countrate_pct

        scaled_transmission = min(1, (transmission * scale_factor) / 100)
        self.log.info(f"Scaled transmission is : {scaled_transmission}")

        self.recwrap.send_to(
            "strategy",
            {"parameters": {"scaled_transmission": float(scaled_transmission)}},
        )

        results_directory.mkdir(parents=True, exist_ok=True)
        log_file_name = [
            f for f in working_directory.glob("*.out") if "slurm" in str(f)
        ][0]
        output_files = {
            "dials.find_spots.log": "dials.find_spots.log",
            log_file_name: "estimate_transmission.log",
        }
        for src_file_name, dest_file_name in output_files.items():
            source_file = working_directory / src_file_name
            destination = results_directory / dest_file_name

            if not source_file.exists():
                self.log.info(f"{source_file=} does not exsist")
                return False

            self.log.info(f"Copying {str(source_file)} to {str(destination)}")
            shutil.copy(source_file, destination)

        self.draw_plot(num_counts, num_pixels)
        self.save_hist_to_json(counts_hist, trusted_range, results_directory)

        self.log.info("Done.")
        return True

    def collect_counts_from_reflections(self, reflections):
        "Iterate through the shoeboxes to a reflection and generate a pixel histogram"

        shoeboxes = reflections["shoebox"]
        counter = Counter()
        for sbox in shoeboxes:
            counter.update(sbox.data.as_numpy_array().ravel())

        sorted_counter = sorted(counter.items())
        return {str(int(k)): v for k, v in sorted_counter}

    def get_percentile_index(self, num_pixels, percentile):
        threshold = sum(num_pixels) * percentile

        for i, cum_sum in enumerate(accumulate(num_pixels)):
            if cum_sum >= threshold:
                return i

        return len(num_pixels)

    def save_hist_to_json(self, hist, max_trusted_value, results_dir):
        results_path = results_dir / "overload.json"
        self.log.info(f"Saving counts histogram to {str(results_path)}")
        with open(results_path, "w") as f:
            json.dump(
                {"counts": hist, "overload_limit": max_trusted_value}, f, indent=2
            )

        self.log.info("Saved.")

    def draw_plot(self, counts, pixels):
        """Create ASCII art histogram"""

        self.log.info("Plotting pixel intensities...")
        
        width, height = 60, 20
        title = "'Pixel intensity distribution'"
        xlabel = "'Num counts'"
        ylabel = "'Counts'"

        command = ["gnuplot"]
        plot_commands = [
            "set term dumb %d %d" % (width, height - 2),
            "set logscale y",
            "set logscale x",
            "set ytics out",
            "set title %s" % title,
            "set xlabel %s" % xlabel,
            "set ylabel %s offset character %d,0" % (ylabel, len(ylabel) // 2),
        ]

        data_string = "\n".join(f"{x} {y}" for x, y in zip(counts[1:], pixels[1:]))

        plot_commands.append("plot '-' using 1:2 title '' with lines")
        plot_commands.append(data_string)

        try:
            result = subprocess.run(
                command, input="\n".join(plot_commands), text=True, capture_output=True
            )
        except (OSError, subprocess.TimeoutExpired) as e:
            self.log.info("Error plotting counts vs pixels")
            self.log.info(e)
            return
        else:
            self.log.info(result.stdout)
