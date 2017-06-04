from __future__ import absolute_import, division

class SimpleStrategy():

  def __init__(self, service_name, minimum=None, maximum=None):
    self.minimum = minimum
    self.maximum = maximum
    self.service_name = service_name

  def assess(self, environment):

    assert isinstance(environment, dict), 'passed environment is invalid'

    recommendation = {
      'increase': {},
      'decrease': {},
      'shutdown': [],
    }

    instances = environment.get('running_services', {}).get(self.service_name, [])

    if self.minimum and len(instances) < self.minimum:
      recommendation['increase'][self.service_name] = \
          { 'instances': self.minimum - len(instances) }

    if self.maximum and len(instances) > self.maximum:
      recommendation['decrease'][self.service_name] = \
          { 'instances': len(instances) - self.maximum }

    return recommendation
