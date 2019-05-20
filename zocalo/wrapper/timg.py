from __future__ import absolute_import, division, print_function

import logging
import os

import procrunner
import zocalo.wrapper

logger = logging.getLogger("dlstbx.wrap.timg")


class TopazWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        # run dozor in working directory
        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = params["working_directory"]
        if not os.path.exists(working_directory):
            os.makedirs(working_directory)
        os.chdir(working_directory)

        logger.info("Running Tim's stuff")

        result = procrunner.run(
            ["/dls/science/users/riw56156/dls_topaz3/timscript"],
            timeout=params.get("timeout", 3600),
        )

        logger.info("command: %s", " ".join(result["command"]))
        logger.info("timeout: %s", result["timeout"])
        logger.info("time_start: %s", result["time_start"])
        logger.info("time_end: %s", result["time_end"])
        logger.info("runtime: %s", result["runtime"])
        logger.info("exitcode: %s", result["exitcode"])
        logger.debug(result["stdout"])
        logger.debug(result["stderr"])

        # self.recwrap.send_to(
        #         "image-analysis-results",
        #         {"file-number": image, "dozor_score": tuple(results[image])[0]},
        # )

        return result["exitcode"] == 0
