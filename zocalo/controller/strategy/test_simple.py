from __future__ import absolute_import, division

from dlstbx.zocalo.controller.strategy.simple import SimpleStrategy
import mock
import uuid

service = mock.sentinel.service_name

no_action = {
    'decrease': {},
    'increase': {},
    'shutdown': [],
}

request_spin_up = lambda n: {
    'decrease': {},
    'increase': { service: { 'instances' : n } },
    'shutdown': [],
}

request_shut_down = lambda n: {
    'decrease': { service: { 'instances' : n } },
    'increase': {},
    'shutdown': [],
}

def mock_environment(number_of_services_running):
  environment = { 'running_services': { } }
  if number_of_services_running:
    environment['running_services'][service] = []
    for i in range(number_of_services_running):
      environment['running_services'][service].append({ 'host': str(uuid.uuid4()) })
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

  assert strategy.assess({}) == request_spin_up(2)
  assert strategy.assess(mock_environment(0)) == request_spin_up(2)
  assert strategy.assess(mock_environment(1)) == request_spin_up(1)
  assert strategy.assess(mock_environment(2)) == no_action
  assert strategy.assess(mock_environment(3)) == no_action
  assert strategy.assess(mock_environment(4)) == no_action

def test_fewer_instances_are_requested_when_above_maximum():
  strategy = SimpleStrategy(service, maximum=2)

  assert strategy.assess({}) == no_action
  assert strategy.assess(mock_environment(0)) == no_action
  assert strategy.assess(mock_environment(1)) == no_action
  assert strategy.assess(mock_environment(2)) == no_action
  assert strategy.assess(mock_environment(3)) == request_shut_down(1)
  assert strategy.assess(mock_environment(4)) == request_shut_down(2)

def test_adjust_instances_when_outside_boundaries():
  strategy = SimpleStrategy(service, maximum=2, minimum=2)

  assert strategy.assess({}) == request_spin_up(2)
  assert strategy.assess(mock_environment(0)) == request_spin_up(2)
  assert strategy.assess(mock_environment(1)) == request_spin_up(1)
  assert strategy.assess(mock_environment(2)) == no_action
  assert strategy.assess(mock_environment(3)) == request_shut_down(1)
  assert strategy.assess(mock_environment(4)) == request_shut_down(2)
