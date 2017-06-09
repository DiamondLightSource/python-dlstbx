from __future__ import absolute_import, division

import dlstbx.zocalo.controller.strategy

def _filter_active(instances):
  return { host: instance for host, instance in instances.iteritems()
           if instance['status'] in (
               dlstbx.zocalo.controller.strategy.StrategyEnvironment.S_STARTING,
               dlstbx.zocalo.controller.strategy.StrategyEnvironment.S_RUNNING,
               dlstbx.zocalo.controller.strategy.StrategyEnvironment.S_HOLDSHDN,
           ) }

class SimpleStrategy():

  def __init__(self, service_name, minimum=None, maximum=None):
    self.minimum = minimum
    self.maximum = maximum
    self.service_name = service_name

  def assess(self, environment):

    assert isinstance(environment, dict), 'passed environment is invalid'

    result = {
      'required': {},
      'optional': {},
      'shutdown': {},
    }

    instances = _filter_active(environment.get('services', {}).get(self.service_name, {}))

    if self.minimum and len(instances) < self.minimum:
      result['required']['count'] = self.minimum

    if self.maximum and len(instances) > self.maximum:
      result['required']['count'] = self.maximum

    return result
