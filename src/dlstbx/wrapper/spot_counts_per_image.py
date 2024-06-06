from __future__ import annotations

import json
import os
import shutil

import procrunner

from dlstbx.wrapper import Wrapper


class SCPIWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.spot_counts_per_image"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        # run in working directory
        working_directory = params["working_directory"]
        if not os.path.exists(working_directory):
            os.makedirs(working_directory)
        os.chdir(working_directory)

        prefix = "pia"

        if os.getenv("NSLOTS") or params.get("nproc"):
            nproc = ["nproc=" + str(os.getenv("NSLOTS") or params.get("nproc"))]
        else:
            nproc = []

        # Set up PIA parameters
        parameters = params.get("find_spots")
        if parameters:
            parameters = [f"{k}={v}" for k, v in parameters.items()]
        else:
            parameters = ["d_max=40"]

        success = True
        for command in (
            ["dials.import", params["data"]],
            ["dials.find_spots", "imported.expt"] + nproc + parameters,
            [
                "dials.spot_counts_per_image",
                "imported.expt",
                "strong.refl",
                "json=%s.json" % prefix,
                "joint_json=True",
                "split_json=True",
            ],
        ):
            self.log.info("Running command: %r", command)
            result = procrunner.run(command, timeout=params.get("timeout"))

            self.log.info("runtime: %s", result["runtime"])
            if result["exitcode"] or result["timeout"]:
                self.log.info("timeout: %s", result["timeout"])
                self.log.info("time_start: %s", result["time_start"])
                self.log.info("time_end: %s", result["time_end"])
                self.log.info("exitcode: %s", result["exitcode"])
                self.log.info(result["stdout"])
                self.log.info(result["stderr"])
                self.log.error(
                    "Spot counting failed on %s during step %s",
                    params["data"],
                    command[0],
                )
                success = False
                break

        # copy output files to result directory
        results_directory = params["results_directory"]
        if not os.path.exists(results_directory):
            os.makedirs(results_directory)

        defaultfiles = ("estimated_d_min", "n_spots_total")
        foundfiles = []
        filesmissing = False
        for filename in params.get("keep_files", defaultfiles):
            filename = prefix + "_" + filename + ".json"

            if os.path.exists(filename):
                dst = os.path.join(results_directory, filename)
                self.log.debug(f"Copying {filename} to {dst}")
                shutil.copy(filename, dst)
                foundfiles.append(dst)
                self.record_result_individual_file(
                    {
                        "file_path": results_directory,
                        "file_name": filename,
                        "file_type": "pia",
                    }
                )
            else:
                filesmissing = True
                if success:
                    self.log.warning("Expected output file %s missing", filename)
                else:
                    self.log.info("Expected output file %s missing", filename)
        success = success and not filesmissing

        if foundfiles:
            self.log.info("Notifying for found files: %s", str(foundfiles))
            self.record_result_all_files({"filelist": foundfiles})

        # Identify selection of PIA results to send on
        selections = [
            k
            for k in self.recwrap.recipe_step["output"].keys()
            if isinstance(k, str) and k.startswith("select-")
        ]
        selections = {int(k[7:]): k for k in selections}

        self.log.info("Processing grouped per-image-analysis statistics")
        json_data = {"total_intensity": []}
        if os.path.exists("%s.json" % prefix):
            with open("%s.json" % prefix) as fp:
                json_data = json.load(fp)
        pia_keys = json_data.keys()
        imagecount = len(json_data["total_intensity"])
        for filenumber, image_values in enumerate(zip(*json_data.values()), 1):
            pia = dict(zip(pia_keys, image_values))
            pia["file-number"] = filenumber

            # Send result for every image
            self.recwrap.send_to("every", pia)
            print("Every:", pia)

            # Send result for image selections
            for m, dest in selections.items():
                if filenumber in (
                    imagecount,
                    1
                    + round(filenumber * (m - 1) // imagecount) * imagecount // (m - 1),
                ):
                    self.recwrap.send_to(dest, pia)
                    print("Select:", pia)
        self.log.info("Done.")

        return success
