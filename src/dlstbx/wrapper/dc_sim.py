from __future__ import annotations

import logging

import dlstbx.dc_sim.definitions
from dlstbx.wrapper import Wrapper

logger = logging.getLogger("dlstbx.wrap.dc_sim")


class DCSimWrapper(Wrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        beamline = params["beamline"]
        scenario = params["scenario"]
        logger.info(
            "Running simulated data collection '%s' on beamline '%s'",
            scenario,
            beamline,
        )

        # Simulate the data collection
        result = dlstbx.dc_sim.call_sim(test_name=scenario, beamline=beamline)

        if result:
            logger.info(f"Simulated data collection completed with {result!r}")
        else:
            logger.error("Simulated data collection failed")

        self.recwrap.send_to("dc_sim", result._asdict())
        return bool(result)
