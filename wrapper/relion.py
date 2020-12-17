import logging
import os
import pathlib

import dlstbx.util.symlink

# import procrunner
import zocalo.wrapper
import pprint

logger = logging.getLogger("dlstbx.wrap.relion")


class RelionWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = pathlib.Path(params["working_directory"])
        results_directory = pathlib.Path(params["results_directory"])

        # create working directory
        working_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            # Create symbolic link above working directory
            dlstbx.util.symlink.create_parent_symlink(
                str(working_directory), params["create_symlink"]
            )

        # Create a symbolic link in the working directory to the image directory
        movielink = "Movies"
        os.symlink(params["image_directory"], working_directory / movielink)
        params["ispyb_parameters"]["import_images"] = os.path.join(
            movielink, params["file_template"]
        )
        pprint(params["ispyb_parameters"])

        # construct relion command line
        # command = ["relion", params["screen-selection"]]

        # run relion
        # result = procrunner.run(
        #    command,
        #    timeout=params.get("timeout"),
        #    working_directory=working_directory.strpath,
        #    environment_override={"PYTHONIOENCODING": "UTF-8"},
        # )
        # logger.info("command: %s", " ".join(result["command"]))
        # logger.info("exitcode: %s", result["exitcode"])
        # logger.debug(result["stdout"])
        # logger.debug(result["stderr"])
        # success = result["exitcode"] == 0
        success = True

        # copy output files to result directory
        results_directory.mkdir(parents=True, exist_ok=True)

        if params.get("create_symlink"):
            # Create symbolic link above results directory
            dlstbx.util.symlink.create_parent_symlink(
                str(results_directory), params["create_symlink"]
            )

        logger.info("Done.")

        return success
