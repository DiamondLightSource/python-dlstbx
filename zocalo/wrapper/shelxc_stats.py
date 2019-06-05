from __future__ import absolute_import, division, print_function

import zocalo.wrapper
from copy import deepcopy
from dlstbx.util.shelxc import reduce_shelxc_results


class ShelxcStatsWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]

        shelxc_stats = deepcopy(self.recwrap.payload)
        data_stats = reduce_shelxc_results(shelxc_stats, params)
        if data_stats:
            self.recwrap.send_to("downstream", data_stats)
            return True
        return False
