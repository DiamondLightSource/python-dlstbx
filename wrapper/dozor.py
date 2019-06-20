from __future__ import absolute_import, division, print_function

import logging
import os
import re

import procrunner
import zocalo.wrapper
from dlstbx.util.dxtbx_to_dozor import (
    dxtbx_to_dozor,
    parse_dozor_output,
    write_dozor_input,
)
from dxtbx.datablock import DataBlockTemplateImporter

logger = logging.getLogger("dlstbx.wrap.dozor")


class DozorWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        cwd = os.path.abspath(os.curdir)

        # run dozor in working directory
        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = params["working_directory"]
        if not os.path.exists(working_directory):
            os.makedirs(working_directory)
        os.chdir(working_directory)

        # Generate dozor input file
        template = params["pattern"]
        # Convert ISPyB %d pattern to dxtbx #### pattern
        r = re.search("%([0-9]*)d", template)
        if not r:
            logger.error("Could not parse template pattern '%s'", template)
            return
        patternlen = max(int(r.group(1)), 1)
        template = template[: r.start()] + ("#" * patternlen) + template[r.end() :]
        logger.info("Running dozor for '%s'", template)

        start = int(params.get("start", 0))
        end = int(params.get("end", 0))
        importer = DataBlockTemplateImporter([template], 0)
        datablocks = importer.datablocks
        assert len(datablocks) == 1
        datablock = datablocks[0]
        imagesets = datablock.extract_imagesets()
        assert len(imagesets) == 1
        imageset = imagesets[0]

        dozor = dxtbx_to_dozor(imageset)
        dozor["name_template_image"] = imageset.get_template().replace("#", "?")
        if (start > 0) and (end > 0):
            dozor["first_image_number"] = start
            dozor["number_images"] = end - start + 1
        write_dozor_input(dozor, "dozor.in")

        result = procrunner.run(
            ["dozor", "dozor.in"], timeout=params.get("timeout", 3600)
        )

        logger.info("command: %s", " ".join(result["command"]))
        logger.info("timeout: %s", result["timeout"])
        logger.info("time_start: %s", result["time_start"])
        logger.info("time_end: %s", result["time_end"])
        logger.info("runtime: %s", result["runtime"])
        logger.info("exitcode: %s", result["exitcode"])
        logger.debug(result["stdout"])
        logger.debug(result["stderr"])

        os.chdir(cwd)

        # results are a dictionary keyed by image number, containing main score,
        # spot score and observed resolution
        results = parse_dozor_output(result["stdout"])

        for image in sorted(results):
            print("%4d" % image, "%6.3f %6.3f %6.3f" % tuple(results[image]))
            self.recwrap.send_to(
                "image-analysis-results",
                {"file-number": image, "dozor_score": tuple(results[image])[0]},
            )

        logger.info("Dozor done.")

        return result["exitcode"] == 0
