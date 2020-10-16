"""
Wrapper for python-topaz3, to generate predictions from input phase files given the cell info and space groups
All files will be created in the working directory and then the final predictions will be copied into the results
directory, along with a graph file.
"""


import json
import logging
import os
import shutil

import procrunner
import zocalo.wrapper
import tempfile

logger = logging.getLogger("dlstbx.wrap.topaz3")

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
}


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
        ), "Working directory at {0} does not exist".format(params["working_directory"])
        # assert os.path.exists(
        #    params["topaz_python"]
        # ), "Topaz python at {0} does not exist".format(params["topaz_python"])
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
        assert isinstance(
            payload["space_group"], str
        ), "Expected string for space group, got {0}".format(payload["space_group"])
        assert isinstance(
            payload["best_solvent"], str
        ), "Expected string for best_solvent, got {0}".format(payload["best_solvent"])
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
                working_directory, "run_shelxe_{}.sh".format(os.path.basename(fp.name))
            )
            fp.close()
            with open(shelxe_script, "w") as fp:
                fp.writelines(
                    [
                        "#!/bin/bash\n",
                        ". /etc/profile.d/modules.sh\n",
                        "module load ccp4\n",
                        "shelxe {0} {1} -s{2} -m20 -l10 -a3\n".format(
                            os.path.splitext(os.path.basename(hkl_file))[0],
                            os.path.splitext(os.path.basename(fa_file))[0],
                            best_solvent,
                        ),
                        "shelxe {0} {1} -i -s{2} -m20 -l10 -a3\n".format(
                            os.path.splitext(os.path.basename(hkl_file))[0],
                            os.path.splitext(os.path.basename(fa_file))[0],
                            best_solvent,
                        ),
                    ]
                )
        except OSError:
            logger.exception(
                "Could not create shelxe script file in the working directory"
            )
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
            logger.exception(
                "Process returned an error code when running shelxe tracing"
            )
            return True
        except Exception:
            logger.exception("Shelxe tracing script has failed")

        logger.info(f"Using venv with command: source {topaz_python}")

        original_phase_file = os.path.join(
            working_directory, os.path.basename(payload["original_phase_file"])
        )
        inverse_phase_file = os.path.join(
            working_directory, os.path.basename(payload["inverse_phase_file"])
        )
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

        prediction_command = self.build_prediction_command(
            map_original, map_inverse, 20, model_file, working_directory, rgb
        )
        try:
            fp = tempfile.NamedTemporaryFile(dir=working_directory)
            topaz3_script = os.path.join(
                working_directory, "run_topaz3_{}.sh".format(os.path.basename(fp.name))
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
            logger.exception("Could not create topaz3 script file %s", topaz3_script)
        # Run procrunner with a clean python environment to avoid DIALS/topaz3 module clashes
        result = procrunner.run(
            ["sh", topaz3_script],
            timeout=params["timeout"],
            working_directory=working_directory,
            environment_override=clean_environment,
        )
        if result["exitcode"] or result["timeout"]:
            logger.info(
                "Running topaz3 script has failed with exitcode %s", result["exitcode"]
            )
            return True

        self.graph_output(
            (working_directory + "/avg_predictions.json"),
            (working_directory + "/topaz_graph.json"),
        )

        # Create results directory if it does not exist
        if not os.path.exists(results_directory):
            try:
                os.mkdir(results_directory)
            except Exception:
                logger.exception(
                    "Could not create results directory at %s", results_directory
                )
        assert os.path.exists(
            params["results_directory"]
        ), "Results directory at {0} does not exist".format(params["results_directory"])

        # Copy final results to results directory
        logger.info(
            "Copying avg_predictions.json and raw_predictions.json to {0}".format(
                results_directory
            )
        )
        shutil.copy((working_directory + "/avg_predictions.json"), results_directory)
        shutil.copy((working_directory + "/raw_predictions.json"), results_directory)

        return True
