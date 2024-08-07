from __future__ import annotations

from copy import deepcopy

from dlstbx.util.shelxc import reduce_shelxc_results
from dlstbx.wrapper import Wrapper


class ShelxcStatsWrapper(Wrapper):
    _logger_name = "zocalo.wrap.shelxc_stats"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]

        shelxc_stats = deepcopy(self.recwrap.payload)
        try:
            data_stats = reduce_shelxc_results(shelxc_stats, params, self.log)
        except Exception:
            self.log.debug("Cannot process SHELXC results")
            return False
        if not data_stats:
            self.log.debug("SHELXC data not available")
            return False
        self.recwrap.send_to("downstream", data_stats)
        return True
