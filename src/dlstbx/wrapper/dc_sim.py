from __future__ import annotations

import os
import re

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

        # Check that any supplied src_dir is from an allowed visit
        if params["src_dir"] is not None:
            # Extract visit from the source directory path
            m1 = re.search(r"(/dls/(\S+?)/data/\d+/)(\S+)", params["src_dir"])
            if m1:
                subdir = m1.groups()[2]
                m2 = re.search(r"^(\S+?)/", subdir)
                if m2:
                    src_visit = m2.groups()[0]
                elif subdir:
                    src_visit = subdir
            else:
                m1 = re.search(r"(/dls/mx/data/)(\S+)", params["src_dir"])
                if m1:
                    subdir = m1.groups()[1]
                    src_visit = subdir.split(os.sep)[1]
            # Compare to src_allowed_visits
            if not src_visit.startswith(tuple(params["src_allowed_visits"])):
                raise ValueError(f"Supplied src_dir from forbidden visit: {src_visit}")

        # Simulate the data collection
        result = dlstbx.dc_sim.call_sim(
            test_name=params["scenario"],
            beamline=params["beamline"],
            src_dir=params["src_dir"],
            src_prefixes=params["src_prefix"],
            src_run_num=params["src_run_num"],
            sample_id=params["sample_id"],
            dest_visit=params["visit"],
            dflt_proposals=params["dflt_proposals"],
            src_dcid=params["src_dcid"],
        )

        if result:
            self.log.info(f"Simulated data collection completed with {result!r}")
        else:
            self.log.error("Simulated data collection failed")

        self.recwrap.send_to("dc_sim", result._asdict())
        return bool(result)
