from __future__ import annotations

import dlstbx.dc_sim.definitions
from dlstbx.wrapper import Wrapper


class DCSimWrapper(Wrapper):

    _logger_name = "dlstbx.wrap.dc_sim"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        beamline = params["beamline"]
        scenario = params["scenario"]

        # Check for optional parameters specifying custom data
        if "src_dir" in params:
            src_dir = params["src_dir"]
        if "src_prefix" in params:
            src_prefix = params["src_prefix"]
        if "src_run_num" in params:
            src_run_num = params["src_run_num"]
        if "sample_id" in params:
            sample_id = params["sample_id"]

        self.log.info(
            "Running simulated data collection '%s' on beamline '%s'",
            scenario,
            beamline,
        )

        # Simulate the data collection
        if src_dir:
            result = dlstbx.dc_sim.call_sim(
                test_name=scenario,
                beamline=beamline,
                src_dir=src_dir,
                src_prefixes=src_prefix,
                src_run_num=src_run_num,
                sample_id=sample_id,
            )
        else:
            result = dlstbx.dc_sim.call_sim(test_name=scenario, beamline=beamline)

        if result:
            self.log.info(f"Simulated data collection completed with {result!r}")
        else:
            self.log.error("Simulated data collection failed")

        self.recwrap.send_to("dc_sim", result._asdict())
        return bool(result)
