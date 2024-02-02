from __future__ import annotations

import dlstbx.dc_sim.definitions
from dlstbx.wrapper import Wrapper


class DCSimWrapper(Wrapper):

    _logger_name = "dlstbx.wrap.dc_sim"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        # Get parameters from the recipe file (as a copy so that changes can be made without affecting anything)
        params = self.recwrap.recipe_step["job_parameters"].copy()

        # A list of placeholder values to check for in params
        placeholders = [
            "{src_dir}",
            "{src_prefix}",
            "{src_run_num}",
            "{sample_id}",
            "{visit}",
        ]

        # Replace any remaining placeholder values in params with None.
        for key, value in params.items():
            if value in placeholders:
                params[key] = None

        self.log.info(
            "Running simulated data collection '%s' on beamline '%s'",
            params["scenario"],
            params["beamline"],
        )

        # Convert command line input of certain parameters into a list
        for key in ["src_prefix", "src_run_num"]:
            try:
                value = eval(params[key])
                if isinstance(value, list) or isinstance(value, tuple):
                    params[key] = value
                else:
                    params[key] = [
                        value,
                    ]
            except (SyntaxError, NameError, TypeError):
                # Case for dealing with non-evaluatable input (e.g. string)
                if params[key] is not None:
                    params[key] = [
                        params[key],
                    ]

        # Simulate the data collection
        result = dlstbx.dc_sim.call_sim(
            test_name=params["scenario"],
            beamline=params["beamline"],
            src_dir=params["src_dir"],
            src_prefixes=params["src_prefix"],
            src_run_num=params["src_run_num"],
            sample_id=params["sample_id"],
            dest_visit=params["visit"],
        )

        if result:
            self.log.info(f"Simulated data collection completed with {result!r}")
        else:
            self.log.error("Simulated data collection failed")

        self.recwrap.send_to("dc_sim", result._asdict())
        return bool(result)
