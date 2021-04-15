import logging
import time

import dlstbx.em_sim
import zocalo.wrapper

logger = logging.getLogger("dlstbx.wrap.em_sim")


class EMSimWrapper(zocalo.wrapper.BaseWrapper):
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

        start = time.time()

        # Simulate the data collection
        dcids = dlstbx.em_sim.call_sim(test_name=scenario, beamline=beamline)

        result = {
            "beamline": beamline,
            "scenario": scenario,
            "DCIDs": dcids,
            "time_start": start,
            "time_end": time.time(),
        }

        if dcids:
            logger.info(
                "Simulated data collection completed with result:\n%s", repr(result)
            )
        else:
            logger.error("Simulated data collection failed")

        self.recwrap.send_to("em_sim", result)
        return bool(dcids)
