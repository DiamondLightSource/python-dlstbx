from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from pprint import pformat

import xmltodict

import dlstbx.util.symlink
from dlstbx.util import iris
from dlstbx.util.iris import write_singularity_script
from dlstbx.wrapper import Wrapper


class FastEPWrapper(Wrapper):

    _logger_name = "zocalo.wrap.fast_ep"

    def stop_fast_ep(self, params):
        """Decide whether to run fast_ep or not based on the completeness, dI/s(dI) and
        resolution of actual data."""

        from iotbx.reflection_file_reader import any_reflection_file

        if "go_fast_ep" not in params:
            self.log.info("go_fast_ep settings not available")
            return False

        thres_d_min = params["go_fast_ep"].get("d_min", -1)

        def check_thresholds(data, threshold):
            thres_completeness = threshold.get("completeness", -1)
            thres_dIsigdI = threshold.get("dI/sigdI", -1)

            differences = data.anomalous_differences()
            dIsigdI = sum(abs(differences.data())) / sum(differences.sigmas())
            completeness = data.completeness()
            if completeness < thres_completeness:
                self.log.info(
                    "Data completeness %.2f below threshold value %.2f. Aborting.",
                    completeness,
                    thres_completeness,
                )
                return True
            if dIsigdI < thres_dIsigdI:
                self.log.info(
                    "Data dI/s(dI) %.2f below threshold value %.2f. Aborting.",
                    dIsigdI,
                    thres_dIsigdI,
                )
                return True
            self.log.info(
                "Data completeness: %.2f  threshold: %.2f",
                completeness,
                thres_completeness,
            )
            self.log.info(
                f"Data dI/s(dI): {dIsigdI:.2f}  threshold: {thres_dIsigdI:.2f}"
            )
            return False

        hkl_file = any_reflection_file(params["data"])
        mas = hkl_file.as_miller_arrays()
        try:
            all_data = next(m for m in mas if m.anomalous_flag())
        except StopIteration:
            self.log.exception(f"No anomalous data found in {str(params['data'])}")
            return True
        if all_data.d_min() > thres_d_min:
            select_data = all_data
            res = check_thresholds(select_data, params["go_fast_ep"].get("low_res", {}))
        else:
            select_data = all_data.resolution_filter(d_min=thres_d_min)
            res = check_thresholds(
                select_data, params["go_fast_ep"].get("high_res", {})
            )
        return res

    def construct_commandline(self, params):
        """Construct fast_ep command line.
        Takes job parameter dictionary, returns array."""

        command = ["fast_ep"]
        for param, value in params["fast_ep"].items():
            if value:
                self.log.info(f"Parameter {param}: {value}")
                if param == "rlims":
                    value = ",".join(str(r) for r in value)
                command.append(f"{param}={value}")

        return command

    def send_results_to_ispyb(self, xml_file):
        params = self.recwrap.recipe_step["job_parameters"]

        scaling_id = params.get("ispyb_parameters", params).get("scaling_id", None)
        if not str(scaling_id).isdigit():
            self.log.error(
                f"Can not write results to ISPyB: no scaling ID set ({scaling_id})"
            )
            return False
        scaling_id = int(scaling_id)
        self.log.info(
            f"Inserting fast_ep phasing results from {xml_file} into ISPyB for scaling_id {scaling_id}"
        )

        phasing_results = xmltodict.parse(xml_file.read_text())

        self.log.info(
            f"Sending {phasing_results} phasing results commands to ISPyB for scaling_id {scaling_id}"
        )
        self.recwrap.send_to(
            "ispyb",
            {
                "phasing_results": phasing_results,
                "scaling_id": scaling_id,
            },
        )
        return True

    def setup(self, working_directory, params):
        if params.get("ispyb_parameters"):
            if params["ispyb_parameters"].get("data"):
                params["data"] = params["ispyb_parameters"]["data"]
            if int(
                params["ispyb_parameters"].get("check_go_fast_ep", False)
            ) and self.stop_fast_ep(params):
                self.log.info("Skipping fast_ep (go_fast_ep == No)")
                return False

        # Create working directory with symbolic link
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory, params["create_symlink"], levels=1
            )

        singularity_image = params.get("singularity_image")
        if singularity_image:
            try:
                # shutil.copy(singularity_image, str(working_directory))
                # image_name = Path(singularity_image).name
                write_singularity_script(working_directory, singularity_image)
                self.recwrap.environment.update(
                    {"singularity_image": singularity_image}
                )
            except Exception:
                self.log.exception("Error writing singularity script")
                return False

        return True

    def run_fast_ep(self, working_directory, params):
        if params.get("ispyb_parameters"):
            if params["ispyb_parameters"].get("data"):
                if "singularity_image" in self.recwrap.environment:
                    params["fast_ep"]["data"] = str(
                        working_directory
                        / Path(params["ispyb_parameters"]["data"]).name
                    )
                else:
                    params["fast_ep"]["data"] = params["ispyb_parameters"]["data"]

        command = self.construct_commandline(params)
        subprocess_directory = working_directory / params["create_symlink"]
        subprocess_directory.mkdir(parents=True, exist_ok=True)

        try:
            start_time = time.perf_counter()
            self.log.info("command: %s", " ".join(command))
            result = subprocess.run(
                command,
                timeout=params.get("timeout"),
                cwd=subprocess_directory,
            )
            runtime = time.perf_counter() - start_time
            self.log.info(f"runtime: {runtime}")
        except subprocess.TimeoutExpired as te:
            success = False
            self.log.warning(f"fast_ep timed out: {te.timeout}\n  {te.cmd}")
            self.log.debug(te.stdout)
            self.log.debug(te.stderr)
        else:
            success = (
                not result.returncode
                and not Path(subprocess_directory / "fast_ep.error").exists()
            )
            if success:
                self.log.info("fast_ep successful")
            else:
                self.log.info(f"fast_ep failed with exitcode {result.returncode}")
                self.log.debug(result.stdout)
                self.log.debug(result.stderr)
        if minio_client := params.get("minio_client"):
            try:
                iris.store_results_in_s3(
                    minio_client,
                    params["bucket_name"],
                    params["rpid"],
                    subprocess_directory,
                    self.log,
                )
            except Exception:
                self.log.exception(
                    "Error while trying to save fast_ep processing results to S3 Echo"
                )
        return success

    def run_report(self, working_directory, params):
        if minio_client := params.get("minio_client"):
            iris.retrieve_results_from_s3(
                minio_client,
                params["bucket_name"],
                working_directory,
                params["rpid"],
                params.get("create_symlink", "fast_ep"),
                self.log,
            )
        # Send results to topaz for hand determination
        working_directory = working_directory / params.get("create_symlink", "fast_ep")
        fast_ep_data_json = working_directory / "fast_ep_data.json"
        if fast_ep_data_json.is_file():
            with fast_ep_data_json.open("r") as fp:
                fast_ep_data = json.load(fp)
            with open(Path(working_directory / "fast_ep.log")) as fp:
                for line in fp:
                    if "Unit cell:" in line:
                        cell_info = tuple(float(v) for v in line.split()[2:])
                        break
            best_sg = fast_ep_data["_spacegroup"][0]
            best_solv = f"{fast_ep_data['solv']:.2f}"
            original_hand = str(working_directory / best_solv / "sad.phs")
            inverted_hand = str(working_directory / best_solv / "sad_i.phs")
            hkl_data = str(working_directory / best_solv / "sad.hkl")
            fa_data = str(working_directory / best_solv / "sad_fa.hkl")
            res_data = str(working_directory / best_solv / "sad_fa.res")
            topaz_data = {
                "original_phase_file": original_hand,
                "inverse_phase_file": inverted_hand,
                "hkl_file": hkl_data,
                "fa_file": fa_data,
                "res_file": res_data,
                "space_group": best_sg,
                "cell_info": cell_info,
                "best_solvent": best_solv,
            }
            self.log.info(f"Topaz data: {pformat(topaz_data)}")
            self.recwrap.send_to("topaz", topaz_data)
        else:
            self.log.warning(
                f"fast_ep failed. Results file {str(fast_ep_data_json)} unavailable"
            )
            return False

        # Create results directory and symlink if they don't already exist
        try:
            results_directory = Path(params["results_directory"]) / params.get(
                "create_symlink", "fast_ep"
            )
            results_directory.mkdir(parents=True, exist_ok=True)
            if params.get("create_symlink"):
                dlstbx.util.symlink.create_parent_symlink(
                    results_directory, params["create_symlink"]
                )

            self.log.info(
                f"Copying fast_ep results to {str(results_directory)}",
            )
            keep_ext = {
                ".cif": "result",
                ".error": "log",
                ".hkl": "result",
                ".html": "log",
                ".ins": "result",
                ".json": "result",
                ".lst": "log",
                ".mtz": "result",
                ".pdb": "result",
                ".png": None,
                ".sca": "result",
                ".sh": None,
                ".xml": False,
            }
            keep = {"fast_ep.log": "log", "shelxc.log": "log"}
            allfiles = []
            for filename in working_directory.iterdir():
                filetype = keep_ext.get(filename.suffix)
                if filename.name in keep:
                    filetype = keep[filename.name]
                if filetype is None:
                    continue
                destination = results_directory / filename.name
                shutil.copy(filename, destination)
                allfiles.append(str(destination))
                if filetype:
                    self.record_result_individual_file(
                        {
                            "file_path": str(destination.parent),
                            "file_name": destination.name,
                            "file_type": filetype,
                        }
                    )

            if "xml" in params["fast_ep"]:
                xml_file = working_directory / params["fast_ep"]["xml"]
                if xml_file.is_file():
                    xml_data = Path(
                        working_directory / params["fast_ep"]["xml"]
                    ).read_text()
                    self.log.info("Sending fast_ep phasing results to ISPyB")
                    xml_file.write_text(
                        xml_data.replace(str(working_directory), str(results_directory))
                    )
                    result_ispyb = self.send_results_to_ispyb(xml_file)
                    if not result_ispyb:
                        self.log.error(
                            "Running phasing2ispyb.py script returned non-zero exit code"
                        )
                else:
                    self.log.info(
                        "fast_ep failed, no .xml output, thus not reporting to ISPyB"
                    )
                    return False
        except KeyError:
            self.log.info(
                "Copying fast_ep results ignored. Results directory unavailable."
            )

        return True

    def run(self):

        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = dict(self.recwrap.recipe_step["job_parameters"])

        # Create working directory with symbolic link
        working_directory = Path(params.get("working_directory", os.getcwd()))
        working_directory.mkdir(parents=True, exist_ok=True)

        if params.get("s3echo"):
            params["minio_client"] = iris.get_minio_client(
                params["s3echo"]["configuration"]
            )
            params["bucket_name"] = params["s3echo"].get("bucket", "fast-ep")

        stage = params.get("stage")
        assert stage in {None, "setup", "run", "report"}
        success = True

        if stage in {None, "setup"}:
            success = self.setup(working_directory, params)

        if stage in {None, "run"} and success:
            success = self.run_fast_ep(working_directory, params)

        if stage in {None, "report"} and success:
            success = self.run_report(working_directory, params)

        return success
