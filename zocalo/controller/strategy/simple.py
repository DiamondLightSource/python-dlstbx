from __future__ import absolute_import, division

import dlstbx.zocalo.controller.strategyenvironment

def _filter_active(instances):
  return { host: instance for host, instance in instances.iteritems()
           if instance['status'] in (
               dlstbx.zocalo.controller.strategyenvironment.StrategyEnvironment.S_STARTING,
               dlstbx.zocalo.controller.strategyenvironment.StrategyEnvironment.S_RUNNING,
               dlstbx.zocalo.controller.strategyenvironment.StrategyEnvironment.S_HOLDSHDN,
           ) }

class SimpleStrategy():

  def __init__(self, service=None, minimum=None, maximum=None, **kwargs):
    self.minimum = minimum
    self.maximum = maximum
    self.service_name = service

    assert self.service_name, 'service name not defined'
    if minimum:
      assert int(minimum) >= 0, 'minimum instances of service %s must be a positive number' % service
    if maximum:
      assert int(maximum) >= 0, 'maximum instances of service %s must be a positive number' % service
    if minimum and maximum:
      assert int(minimum) <= int(maximum), 'minimum instances of service %s must be below or equal to maximum' % service

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
