from __future__ import annotations

from pathlib import Path

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
            "{src_dcid}",
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
            except (SyntaxError, NameError, TypeError):
                # Case for dealing with non-evaluatable input (e.g. string)
                if params[key] is not None:
                    params[key] = [
                        params[key],
                    ]
            else:
                # Case for dealing with lists or tuples
                if isinstance(value, list) or isinstance(value, tuple):
                    params[key] = value
                # Case for dealing with other evaluatable input (e.g. ints)
                else:
                    params[key] = [
                        value,
                    ]

        # Convert parameters into correct format
        if params["src_dir"] is not None:
            params["src_dir"] = Path(params["src_dir"])
        for key in ["sample_id", "src_dcid"]:
            if params[key] is not None:
                params[key] = int(params[key])

        # Simulate the data collection
        result = dlstbx.dc_sim.call_sim(
            test_name=params["scenario"],
            beamline=params["beamline"],
            _src_dir=params["src_dir"],
            _src_prefixes=params["src_prefix"],
            _src_run_num=params["src_run_num"],
            _sample_id=params["sample_id"],
            dest_visit=params["visit"],
            src_dcid=params["src_dcid"],
        )

        if result:
            self.log.info(f"Simulated data collection completed with {result!r}")
        else:
            self.log.error("Simulated data collection failed")

        self.recwrap.send_to("dc_sim", result._asdict())
        return bool(result)
