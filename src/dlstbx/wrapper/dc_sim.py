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
            "{src_dcid}",
            "{src_dcg}",
            "{is_dcg}",
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
        # Check that supplied destination visit is allowed
        if params["visit"] is not None:
            if not params["visit"].startswith(tuple(params["dest_allowed_visits"])):
                raise ValueError(f"{params['visit']} is not an allowed visit")

        # Set whether to output as dcg
        is_dcg = params["src_dcg"] is not None
        if params["is_dcg"] is not None:
            # Check that is_dcg is an acceptable boolean value
            if params["is_dcg"].lower() in ["true", "1", "y", "yes"]:
                is_dcg = True
            elif params["is_dcg"].lower() in ["false", "0", "n", "no"]:
                is_dcg = False
            else:
                raise ValueError(
                    f"{params['is_dcg']} is not a valid value for is_dcg. Acceptable values are 'true', '1', 'y', 'yes' for True and 'false', '0', 'n', 'no' for False - case insensitive"
                )

        # Simulate the data collection
        result = dlstbx.dc_sim.call_sim(
            test_name=params["scenario"],
            beamline=params["beamline"],
            src_dir=params["src_dir"],
            src_prefix=params["src_prefix"],
            src_run_num=params["src_run_num"],
            sample_id=params["sample_id"],
            dest_visit=params["visit"],
            dflt_proposals=params["dflt_proposals"],
            src_dcid=params["src_dcid"],
            src_allowed_visits=params["src_allowed_visits"],
            is_dcg=is_dcg,
            src_dcg=params["src_dcg"],
        )

        if result:
            self.log.info(f"Simulated data collection completed with {result!r}")
        else:
            self.log.error("Simulated data collection failed")

        self.recwrap.send_to("dc_sim", result._asdict())
        return bool(result)
