from __future__ import division

import dlstbx.workflow.services
import dlstbx.workflow.services.sample_services
import mock
import Queue

def test_service_can_be_looked_up():
  '''Attempt to look up the service by its name'''
  service_class = dlstbx.workflow.services.lookup('sample_generator')
  assert service_class == dlstbx.workflow.services.sample_services.Generator

