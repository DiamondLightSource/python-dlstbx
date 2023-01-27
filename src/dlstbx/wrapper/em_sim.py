from __future__ import annotations

import time

import dlstbx.em_sim
from dlstbx.wrapper import Wrapper


class EMSimWrapper(Wrapper):

    _logger_name = "dlstbx.wrap.em_sim"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        beamline = params["beamline"]
        scenario = params["scenario"]
        self.log.info(
            "Running simulated data collection '%s' on beamline '%s'",
            scenario,
            beamline,
        )

        start = time.time()

        # Simulate the data collection
        dcids, pjids = dlstbx.em_sim.call_sim(test_name=scenario, beamline=beamline)

        result = {
            "beamline": beamline,
            "scenario": scenario,
            "DCIDs": dcids,
            "JobIDs": pjids,
            "time_start": start,
            "time_end": time.time(),
        }

        if dcids:
            self.log.info(
                "Simulated data collection completed with result:\n%s", repr(result)
            )
        else:
            self.log.error("Simulated data collection failed")

        self.recwrap.send_to("dc_sim", result)
        return bool(dcids)
