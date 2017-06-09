from __future__ import absolute_import, division

from dlstbx.zocalo.controller.strategy.simple import SimpleStrategy
from dlstbx.zocalo.controller.strategy import StrategyEnvironment
import mock
import uuid

service = mock.sentinel.service_name

no_action = {
    'required': {},
    'optional': {},
    'shutdown': {},
}

request = lambda n: {
    'required': { 'count': n },
    'optional': {},
    'shutdown': {},
}

def mock_environment(number_of_services_running):
  environment = { 'services': {}, 'instances': {} }
  if number_of_services_running:
    environment['services'][service] = {}
    for i in range(number_of_services_running):
      svc_id = str(uuid.uuid4())
      instance = { 'host': str(uuid.uuid4()), 'status': StrategyEnvironment.S_RUNNING }
      environment['services'][service][svc_id] = instance
      environment['instances'][svc_id] = instance
  return environment

def test_simplest_strategy_takes_no_action():
  strategy = SimpleStrategy(service)

  assert strategy.assess({}) == no_action
  assert strategy.assess(mock_environment(0)) == no_action
  assert strategy.assess(mock_environment(1)) == no_action
  assert strategy.assess(mock_environment(2)) == no_action
  assert strategy.assess(mock_environment(3)) == no_action
  assert strategy.assess(mock_environment(4)) == no_action

def test_more_instances_are_requested_when_below_minimum():
  strategy = SimpleStrategy(service, minimum=2)

  assert strategy.assess({}) == request(2)
  assert strategy.assess(mock_environment(0)) == request(2)
  assert strategy.assess(mock_environment(1)) == request(2)
  assert strategy.assess(mock_environment(2)) == no_action
  assert strategy.assess(mock_environment(3)) == no_action
  assert strategy.assess(mock_environment(4)) == no_action

def test_fewer_instances_are_requested_when_above_maximum():
  strategy = SimpleStrategy(service, maximum=2)

  assert strategy.assess({}) == no_action
  assert strategy.assess(mock_environment(0)) == no_action
  assert strategy.assess(mock_environment(1)) == no_action
  assert strategy.assess(mock_environment(2)) == no_action
  assert strategy.assess(mock_environment(3)) == request(2)
  assert strategy.assess(mock_environment(4)) == request(2)

def test_adjust_instances_when_outside_boundaries():
  strategy = SimpleStrategy(service, maximum=2, minimum=2)

  assert strategy.assess({}) == request(2)
  assert strategy.assess(mock_environment(0)) == request(2)
  assert strategy.assess(mock_environment(1)) == request(2)
  assert strategy.assess(mock_environment(2)) == no_action
  assert strategy.assess(mock_environment(3)) == request(2)
  assert strategy.assess(mock_environment(4)) == request(2)
