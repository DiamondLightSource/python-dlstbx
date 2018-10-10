from __future__ import absolute_import, division, print_function

import logging

import dlstbx.dc_sim
import dlstbx.zocalo.wrapper

logger = logging.getLogger('dlstbx.wrap.dc_sim')

class DCSimWrapper(dlstbx.zocalo.wrapper.BaseWrapper):
  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    beamline = params['beamline']
    scenario = params['scenario']
    logger.info("Running simulated data collection '%s' on beamline '%s'", scenario, beamline)

    # Simulate the data collection
    dcids = dlstbx.dc_sim.call_sim(test_name=scenario, beamline=beamline)

    result = { 'beamline': beamline, 'scenario': scenario, 'DCIDs': dcids }

    if dcids:
      logger.info("Simulated data collection completed with result:\n%s", repr(result))
      self.success(result)
    else:
      logger.warn("Simulated data collection failed")
      self.failure(result)

    return bool(dcids)
