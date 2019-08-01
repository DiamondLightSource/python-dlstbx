"""
Wrapper for python-topaz3, to generate predictions from input phase files given the cell info and space groups
All files will be created in the working directory and then the final predictions will be copied into the results
directory, along with a graph file.
"""

from __future__ import absolute_import, division, print_function

import json
import logging
import os
import shutil

import procrunner
import zocalo.wrapper

logger = logging.getLogger("dlstbx.wrap.topaz3")


class Topaz3Wrapper(zocalo.wrapper.BaseWrapper):
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
            "'{0}',"
            "{1},"
            "'{2}',"
            "[200, 200, 200],"
            "'{3}'"
            ")".format(phase_file, cell_info, space_group, output_file)
        )
        return command

    def build_prediction_command(
        self, original_map, inverse_map, slices_per_axis, model_file, output_dir, rgb
    ):
        """Create a python command to perform the phase transformation"""
        command = (
            "from topaz3.predictions import predict_original_inverse;"
            "predict_original_inverse("
            "'{0}',"
            "'{1}',"
            "{2},"
            "'{3}',"
            "'{4}',"
            "rgb={5}"
            ")".format(
                original_map, inverse_map, slices_per_axis, model_file, output_dir, rgb
            )
        )
        return command

    def graph_output(self, avg_predictions_file, output_file):
        """Extract information from the average predictions file and generate graph output"""
        with open(avg_predictions_file) as fp:
            avg_predictions = json.load(fp)
        graph_data = {
            "data": [
                {
                    "name": "Original Hand",
                    "type": "scatter",
                    "x": [0, 1],
                    "y": [
                        avg_predictions["Original"]["0"],
                        avg_predictions["Original"]["1"],
                    ],
                },
                {
                    "name": "Inverse Hand",
                    "type": "scatter",
                    "x": [0, 1],
                    "y": [
                        avg_predictions["Inverse"]["0"],
                        avg_predictions["Inverse"]["1"],
                    ],
                },
            ],
            "layout": {"title": {"text": "Topaz Results"}},
        }
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
        ), "Working directory at {0} does not exist".format(params["working_directory"])
        assert os.path.exists(
            params["topaz_python"]
        ), "Topaz python at {0} does not exist".format(params["topaz_python"])
        assert os.path.exists(
            params["model_file"]
        ), "Model file at {0} does not exist".format(params["model_file"])
        working_directory = params["working_directory"]
        results_directory = params["results_directory"]
        topaz_python = params["topaz_python"]
        model_file = params["model_file"]
        if "rgb" in params:
            assert isinstance(
                params["rgb"], bool
            ), "Expected boolean for rgb, got {0} of type {1}".format(
                params["rgb"], type(params["rgb"])
            )
        else:
            rgb = False
        # Create results directory if it does not exist
        if not os.path.exists(params["results_directory"]):
            try:
                os.mkdir(params["results_directory"])
            except Exception as e:
                logger.error("Could not create results directory at {0}".format(results_directory))
        assert os.path.exists(
            params["results_directory"]
        ), "Results directory at {0} does not exist".format(params["results_directory"])

        # Collect parameters from payload and check them
        payload = self.recwrap.payload
        assert payload is not None, "Could not find payload"
        assert (
            "original_phase_file" in payload
        ), "Could not find original phase file in payload"
        assert (
            "inverse_phase_file" in payload
        ), "Could not find inverse phase file in payload"
        assert "cell_info" in payload, "Could not find cell info in payload"
        assert "space_group" in payload, "Could not find space group in payload"
        assert os.path.exists(
            payload["original_phase_file"]
        ), "Original phase file at {0} does not exist".format(
            payload["original_phase_file"]
        )
        assert os.path.exists(
            payload["inverse_phase_file"]
        ), "Inverse phase file at {0} does not exist".format(
            payload["inverse_phase_file"]
        )
        assert (
            len(payload["cell_info"]) == 6
        ), "Expected list of 6 numbers for cell info, got {0}".format(
            payload["cell_info"]
        )
        assert isinstance(payload["cell_info"][0], int) or isinstance(
            payload["cell_info"][0], float
        ), "Expected list of 6 numbers for cell info, got {0}".format(
            payload["cell_info"]
        )
        assert isinstance(payload["space_group"], str) or isinstance(
            payload["space_group"], unicode
        ), "Expected string for space group, got {0}".format(payload["space_group"])
        original_phase_file = payload["original_phase_file"]
        inverse_phase_file = payload["inverse_phase_file"]
        cell_info = payload["cell_info"]
        space_group = payload["space_group"]

        logger.info("Using venv with command: source {0}".format(topaz_python))

        # Use procrunner to convert the phase files to map files
        # Create the map output file paths - need this later for prediction
        map_original = working_directory + "/{0}.map".format(
            os.path.splitext(os.path.basename(original_phase_file))[0]
        )
        map_inverse = working_directory + "/{0}.map".format(
            os.path.splitext(os.path.basename(inverse_phase_file))[0]
        )
        # Prepare the commands to be executed
        command_original_phase = self.build_phase_to_map_command(
            original_phase_file, cell_info, space_group, map_original
        )
        command_inverse_phase = self.build_phase_to_map_command(
            inverse_phase_file, cell_info, space_group, map_inverse
        )

        logger.info(
            "Preparing original hand\n"
            'Running command: {0} -c "{1}"'.format(topaz_python, command_original_phase)
        )
        try:
            # Run procrunner with a clean python environment to avoid DIALS/topaz3 module clashes
            result = procrunner.run(
                [topaz_python, "-c", command_original_phase],
                timeout=30,
                environment_override={"PYTHONPATH": ""},
            )
            assert result["exitcode"] == 0
            assert not result["stderr"]
            assert result["timeout"] is False
        except AssertionError as e:
            logging.error(e)
            logger.error("Process returned an error code when converting original hand")
            return False
        except Exception as e:
            logging.error(e)
            logging.error("Error converting original hand")

        logger.info(
            "Preparing inverse hand\n"
            'Running command: {0} -c "{1}"'.format(topaz_python, command_inverse_phase)
        )
        try:
            # Run procrunner with a clean python environment to avoid DIALS/topaz3 module clashes
            result = procrunner.run(
                [topaz_python, "-c", command_inverse_phase],
                timeout=30,
                environment_override={"PYTHONPATH": ""},
            )
            assert result["exitcode"] == 0
            assert not result["stderr"]
            assert result["timeout"] is False
        except AssertionError as e:
            logger.error(e)
            logger.error("Process returned an error code when converting inverse hand")
            return False
        except Exception as e:
            logger.error(e)
            logging.error("Error converting inverse hand")

        # Assuming all has gone smoothly, use procrunner to generate predictions
        prediction_command = self.build_prediction_command(
            map_original, map_inverse, 20, model_file, working_directory, rgb
        )
        logger.info(
            "Generating predictions\n"
            'Running command: {0} -c "{1}"'.format(topaz_python, prediction_command)
        )
        try:
            # Run procrunner with a clean python environment to avoid DIALS/topaz3 module clashes
            result = procrunner.run(
                [topaz_python, "-c", prediction_command],
                timeout=60,
                environment_override={"PYTHONPATH": ""},
                print_stdout=False,
                print_stderr=False,
            )
            assert result["exitcode"] == 0
            # Not checking stderr for this command as it returns tensorflow junk
            assert result["timeout"] is False
        except AssertionError as e:
            logger.error(e)
            logger.error(result)
            logger.error("Process returned an error code when getting predictions")
            return False
        except Exception as e:
            logger.error(e)
            logger.error(result)
            logger.error("Error converting inverse hand")

        # Copy final results to results directory
        logger.info(
            "Copying avg_predictions.json and raw_predictions.json to {0}".format(
                results_directory
            )
        )
        shutil.copy((working_directory + "/avg_predictions.json"), results_directory)
        shutil.copy((working_directory + "/raw_predictions.json"), results_directory)

        logger.info("Generating graph output")
        self.graph_output(
            (working_directory + "/avg_predictions.json"),
            (results_directory + "/topaz_graph.json"),
        )

        return True
