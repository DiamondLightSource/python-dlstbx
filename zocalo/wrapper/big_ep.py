from __future__ import absolute_import, division, print_function

import logging
import py
import re
from datetime import datetime

import dlstbx.util.symlink
import procrunner
import zocalo.wrapper

logger = logging.getLogger("dlstbx.wrap.big_ep")


class BigEPWrapper(zocalo.wrapper.BaseWrapper):
    def construct_commandline(self, params):
        """Construct big_ep command line.
       Takes job parameter dictionary, returns array."""

        command = [
            "sh",
            "/dls_sw/apps/mx-scripts/auto-big-ep/zoc-bigep.sh",
            params["big_ep"]["autoproc_id"],
            "%4d%02d%02d_%02d%02d%02d" % tuple(datetime.now().timetuple()[:6]),
            params["big_ep"]["beamline"],
        ]

        return command

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        visit_match = re.search(r"/([a-z]{2}[0-9]{4,5}-[0-9]+)/", params["results_directory"])
        try:
            visit = visit_match.group(1)
        except AttributeError:
            logger.info("Cannot match visit pattern in path %s", params["results_directory"])
            return
        if True in [pfx in visit for pfx in ("lb", "in", "sw")]:
            logger.info("Skipping big_ep for %s visit", visit)
            return

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])

        # Create working directory with symbolic link
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory.strpath, params["create_symlink"]
            )

        command = self.construct_commandline(params)
        logger.info("command: %s", " ".join(command))
        result = procrunner.run(
            command,
            timeout=params.get("timeout"),
            working_directory=working_directory.strpath,
        )
        if result["exitcode"] or result["timeout"]:
            logger.info("timeout: %s", result["timeout"])
            logger.info("exitcode: %s", result["exitcode"])
            logger.debug(result["stdout"])
            logger.debug(result["stderr"])
        logger.info("runtime: %s", result["runtime"])

        # Create results directory and symlink if they don't already exist
        results_directory.ensure(dir=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                results_directory.strpath, params["create_symlink"]
            )

        # XXX what files do we want to keep?

        return result["exitcode"] == 0
