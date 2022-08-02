"""
Wrapper for python-topaz3, to generate predictions from input phase files given the cell info and space groups
All files will be created in the working directory and then the final predictions will be copied into the results
directory, along with a graph file.
"""


from __future__ import annotations

import json
import os
import shutil
import tempfile

import procrunner

from dlstbx.wrapper import Wrapper

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
}


class Topaz3Wrapper(Wrapper):

    _logger_name = "dlstbx.wrap.topaz3"

    """
    Converts phase files into map files, obtains predictions from map files,

    Inputs:

    job_parameters:
        working_directory: str - store intermediate results files here
        results_directory: str - store final results files here
        topaz_python: str - path to python 3 interpreter with python-topaz3 installed
        model_file: str - path to .h5 file containing full Keras model for prediction
        rgb: bool - optional parameter if model requires 3 channel input
    payload:
        original_phase_file: str - original hand phase file to predict
        inverse_phase_file: str - inverse hand phase file to predict
        cell_info: [float, float, float, float, float, float] - cell information in list e.g [10.00, 10.00, 10.00, 90.00, 90.00, 90.00]
        space_group: str - space group for transformation e.g "P212121"
    """

    def build_phase_to_map_command(
        self, phase_file, cell_info, space_group, output_file
    ):
        """Create a python command to perform the phase transformation"""
        command = (
            "from topaz3.conversions import phase_to_map;"
            "phase_to_map("
            f"'{phase_file}',"
            f"{cell_info},"
            f"'{space_group}',"
            "[200, 200, 200],"
            f"'{output_file}')"
        )
        return command

    def build_prediction_command(
        self, original_map, inverse_map, slices_per_axis, model_file, output_dir, rgb
    ):
        """Create a python command to perform the phase transformation"""
        command = (
            "from topaz3.predictions import predict_original_inverse;"
            "predict_original_inverse("
            f"'{original_map}',"
            f"'{inverse_map}',"
            f"{slices_per_axis},"
            f"'{model_file}',"
            f"'{output_dir}',"
            f"rgb={rgb})"
        )
        return command

    def graph_output(self, avg_predictions_file, output_file):
        """Extract information from the average predictions file and generate graph output"""
        with open(avg_predictions_file) as fp:
            avg_predictions = json.load(fp)
        graph_data = {"data": [], "layout": {"title": {"text": "Topaz Results"}}}
        try:
            graph_data["data"].append(
                {
                    "name": "Original Hand",
                    "type": "scatter",
                    "x": [0, 1],
                    "y": [
                        avg_predictions["Original"]["0"],
                        avg_predictions["Original"]["1"],
                    ],
                }
            )
        except KeyError:
            pass
        try:
            graph_data["data"].append(
                {
                    "name": "Inverse Hand",
                    "type": "scatter",
                    "x": [0, 1],
                    "y": [
                        avg_predictions["Inverse"]["0"],
                        avg_predictions["Inverse"]["1"],
                    ],
                }
            )
        except KeyError:
            pass
        # Output to result directory
        with open(output_file, "w") as fp:
            json.dump(graph_data, fp, indent=4, sort_keys=True)
        return True

    def run(self):
        # Check that a recipe wrapper has been passed
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        # Collect the job parameters and check all are valid
        params = self.recwrap.recipe_step["job_parameters"]
        assert params is not None, "Could not find job parameters"
        assert (
            "working_directory" in params
        ), "Could not find working directory in parameters"
        assert (
            "results_directory" in params
        ), "Could not find results directory in parameters"
        assert (
            "topaz_python" in params
        ), "Could not find topaz python interpreter in parameters"
        assert (
            "model_file" in params
        ), "Could not find model file interpreter in parameters"
        assert os.path.exists(
            params["working_directory"]
        ), f"Working directory at {params['working_directory']} does not exist"
        # assert os.path.exists(
        #    params["topaz_python"]
        # ), "Topaz python at {0} does not exist".format(params["topaz_python"])
        assert os.path.exists(
            params["model_file"]
        ), f"Model file at {params['model_file']} does not exist"
        working_directory = params["working_directory"]
        results_directory = params["results_directory"]
        topaz_python = params["topaz_python"]
        model_file = params["model_file"]
        if "rgb" in params:
            assert isinstance(
                params["rgb"], bool
            ), f"Expected boolean for rgb, got {params['rgb']} of type {type(params['rgb'])}"
        else:
            rgb = False

        # Collect parameters from payload and check them
        payload = self.recwrap.payload
        assert payload is not None, "Could not find payload"
        assert (
            "original_phase_file" in payload
        ), "Could not find original phase file in payload"
        assert (
            "inverse_phase_file" in payload
        ), "Could not find inverse phase file in payload"
        assert "hkl_file" in payload, "Could not find hkl data file in payload"
        assert "fa_file" in payload, "Could not find fa data file in payload"
        assert "res_file" in payload, "Could not find res data file in payload"
        assert "cell_info" in payload, "Could not find cell info in payload"
        assert "space_group" in payload, "Could not find space group in payload"
        assert "best_solvent" in payload, "Could not find solvent content in payload"
        assert os.path.exists(
            payload["original_phase_file"]
        ), f"Original phase file at {payload['original_phase_file']} does not exist"
        assert os.path.exists(
            payload["inverse_phase_file"]
        ), f"Inverse phase file at {payload['inverse_phase_file']} does not exist"
        assert (
            len(payload["cell_info"]) == 6
        ), f"Expected list of 6 numbers for cell info, got {payload['cell_info']}"
        assert isinstance(payload["cell_info"][0], int) or isinstance(
            payload["cell_info"][0], float
        ), f"Expected cell info to be int or float types, got {type(payload['cell_info'][0])}"
        assert isinstance(
            payload["space_group"], str
        ), f"Expected string for space group, got {payload['space_group']}"
        assert isinstance(
            payload["best_solvent"], str
        ), f"Expected string for best_solvent, got {payload['best_solvent']}"
        hkl_file = payload["hkl_file"]
        fa_file = payload["fa_file"]
        res_file = payload["res_file"]
        cell_info = payload["cell_info"]
        space_group = payload["space_group"]
        best_solvent = payload["best_solvent"]

        shutil.copy(hkl_file, working_directory)
        shutil.copy(fa_file, working_directory)
        shutil.copy(res_file, working_directory)

        try:
            fp = tempfile.NamedTemporaryFile(dir=working_directory)
            shelxe_script = os.path.join(
                working_directory, f"run_shelxe_{os.path.basename(fp.name)}.sh"
            )
            fp.close()
            with open(shelxe_script, "w") as fp:
                hkl_filename = os.path.splitext(os.path.basename(hkl_file))[0]
                fa_filename = os.path.splitext(os.path.basename(fa_file))[0]
                fp.writelines(
                    [
                        "#!/bin/bash\n",
                        ". /etc/profile.d/modules.sh\n",
                        "module load ccp4\n",
                        f"shelxe {hkl_filename} {fa_filename} -s{best_solvent} -m20 -l10 -a3\n",
                        f"shelxe {hkl_filename} {fa_filename} -i -s{best_solvent} -m20 -l10 -a3\n",
                    ]
                )
        except OSError:
            self.log.exception(
                "Could not create shelxe script file in the working directory"
            )
            return False
        try:
            # Run procrunner with a clean python environment to avoid DIALS/topaz3 module clashes
            result = procrunner.run(
                ["sh", shelxe_script],
                timeout=params["timeout"],
                working_directory=working_directory,
                environment_override=clean_environment,
            )
            assert result["exitcode"] == 0
            assert result["timeout"] is False
        except AssertionError:
            self.log.info("Process returned an error code when running shelxe tracing")
            return True
        except Exception:
            self.log.info("Shelxe tracing script has failed")
            return True

        self.log.info(f"Using venv with command: source {topaz_python}")

        original_phase_file = os.path.join(
            working_directory, os.path.basename(payload["original_phase_file"])
        )
        inverse_phase_file = os.path.join(
            working_directory, os.path.basename(payload["inverse_phase_file"])
        )
        # Create the map output file paths - need this later for prediction
        original_phase_filename = os.path.splitext(
            os.path.basename(original_phase_file)
        )[0]
        inverse_phase_filename = os.path.splitext(os.path.basename(inverse_phase_file))[
            0
        ]
        map_original = os.path.join(working_directory, f"{original_phase_filename}.map")
        map_inverse = os.path.join(working_directory, f"{inverse_phase_filename}.map")
        # Prepare the commands to be executed
        command_original_phase = self.build_phase_to_map_command(
            original_phase_file, cell_info, space_group, map_original
        )
        command_inverse_phase = self.build_phase_to_map_command(
            inverse_phase_file, cell_info, space_group, map_inverse
        )

        prediction_command = self.build_prediction_command(
            map_original, map_inverse, 20, model_file, working_directory, rgb
        )
        try:
            fp = tempfile.NamedTemporaryFile(dir=working_directory)
            topaz3_script = os.path.join(
                working_directory, f"run_topaz3_{os.path.basename(fp.name)}.sh"
            )
            fp.close()
            with open(topaz3_script, "w") as fp:
                fp.writelines(
                    [
                        "#!/bin/bash\n",
                        ". /etc/profile.d/modules.sh\n",
                        f"source {topaz_python}\n",
                        "module load cuda\n",
                        f'python -c "{command_original_phase}"\n',
                        f'python -c "{command_inverse_phase}"\n',
                        f'python -c "{prediction_command}"\n',
                    ]
                )
        except OSError:
            self.log.exception("Could not create topaz3 script file %s", topaz3_script)
            return False
        # Run procrunner with a clean python environment to avoid DIALS/topaz3 module clashes
        result = procrunner.run(
            ["sh", topaz3_script],
            timeout=params["timeout"],
            working_directory=working_directory,
            environment_override=clean_environment,
        )
        if result["exitcode"] or result["timeout"]:
            self.log.info(
                "Running topaz3 script has failed with exitcode %s", result["exitcode"]
            )
            return True

        self.graph_output(
            os.path.join(working_directory, "avg_predictions.json"),
            os.path.join(working_directory, "topaz_graph.json"),
        )

        # Create results directory if it does not exist
        if not os.path.exists(results_directory):
            try:
                os.mkdir(results_directory)
            except Exception:
                self.log.exception(
                    "Could not create results directory at %s", results_directory
                )
        assert os.path.exists(
            params["results_directory"]
        ), f"Results directory at {params['results_directory']} does not exist"

        # Copy final results to results directory
        self.log.info(
            f"Copying avg_predictions.json and raw_predictions.json to {results_directory}"
        )
        shutil.copy(
            os.path.join(working_directory, "avg_predictions.json"), results_directory
        )
        shutil.copy(
            os.path.join(working_directory, "raw_predictions.json"), results_directory
        )

        return True
