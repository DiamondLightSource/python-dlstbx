from __future__ import annotations

import datetime
import logging
import os

import zocalo.wrapper

logger = logging.getLogger("dlstbx.wrap.timg")


class TopazWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        # Check that a recipe wrapper has been passed
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        # Collect the job parameters - working directory and results directory usually expected
        params = self.recwrap.recipe_step["job_parameters"]

        # Move to the working directory
        working_directory = params["working_directory"]
        if not os.path.exists(working_directory):
            os.makedirs(working_directory)
        os.chdir(working_directory)

        # Logs to {name}.e{job_id} in the working directory of the recipe
        logger.info("Running wrapper commands")

        # Write out the parameters to {results_directory}/wrapper_output
        with open("{}/wrapper_output".format(params["results_directory"]), "a+") as f:
            f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            f.write(" Written from simple Zocalo wrapper\n")
            for key, value in params.items():
                f.write(f"{key}: {value}\n")

        logging.info("Wrapper complete")

        return True
