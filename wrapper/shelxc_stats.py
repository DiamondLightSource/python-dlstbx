from __future__ import absolute_import, division, print_function

import zocalo.wrapper
from copy import deepcopy
from dlstbx.util.shelxc import reduce_shelxc_results
import logging


logger = logging.getLogger("dlstbx.wrap.shelxc_stats")


class ShelxcStatsWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]

        shelxc_stats = deepcopy(self.recwrap.payload)
        try:
            data_stats = reduce_shelxc_results(shelxc_stats, params)
        except Exception:
            logger.debug("Cannot process SHELXC results")
            return False
        if not data_stats:
            logger.debug("SHELXC data not available")
            return False
        self.recwrap.send_to("downstream", data_stats)
        return True
