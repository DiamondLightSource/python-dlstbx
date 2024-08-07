from __future__ import annotations

import uuid
from unittest import mock

import pytest
from dlstbx.controller.strategy.simple import SimpleStrategy
from dlstbx.controller.strategyenvironment import StrategyEnvironment

service = mock.sentinel.service_name


def request(n):
    return {"required": {"count": n}, "optional": {}, "shutdown": {}}


def mock_environment(number_of_services_holding, running, disappearing):
    environment = {"services": {}, "instances": {}}
    for status, count in [
        (StrategyEnvironment.S_HOLD, number_of_services_holding),
        (StrategyEnvironment.S_RUNNING, running),
        (StrategyEnvironment.S_SHUTDOWN, disappearing),
    ]:
        if service not in environment["services"]:
            environment["services"][service] = {}
        for i in range(count):
            svc_id = str(uuid.uuid4())
            instance = {"host": str(uuid.uuid4()), "status": status}
            environment["services"][service][svc_id] = instance
            environment["instances"][svc_id] = instance
    return environment


def test_constructor_enforces_sanity():
    with pytest.raises(Exception):
        SimpleStrategy(service=service, minimum=3, maximum=1)
    with pytest.raises(Exception):
        SimpleStrategy(service=service, minimum=-1, maximum=1)
    with pytest.raises(Exception):
        SimpleStrategy(service=service, maximum=-1)
    SimpleStrategy(service=service, minimum=0, maximum=0)
    SimpleStrategy(service=service, maximum=0)


def test_simplest_strategy_holds_at_current_level():
    strategy = SimpleStrategy(service=service)

    assert strategy.assess({}) == request(0)
    assert strategy.assess(mock_environment(0, 0, 0)) == request(0)
    assert strategy.assess(mock_environment(1, 0, 0)) == request(0)
    assert strategy.assess(mock_environment(2, 0, 0)) == request(0)
    assert strategy.assess(mock_environment(3, 0, 0)) == request(0)
    assert strategy.assess(mock_environment(4, 0, 0)) == request(0)
    assert strategy.assess(mock_environment(1, 1, 0)) == request(1)
    assert strategy.assess(mock_environment(2, 2, 0)) == request(2)
    assert strategy.assess(mock_environment(3, 3, 0)) == request(3)
    assert strategy.assess(mock_environment(4, 4, 0)) == request(4)
    assert strategy.assess(mock_environment(0, 1, 0)) == request(1)
    assert strategy.assess(mock_environment(0, 2, 0)) == request(2)
    assert strategy.assess(mock_environment(0, 3, 0)) == request(3)
    assert strategy.assess(mock_environment(0, 4, 0)) == request(4)
    assert strategy.assess(mock_environment(0, 1, 1)) == request(2)
    assert strategy.assess(mock_environment(0, 2, 2)) == request(4)
    assert strategy.assess(mock_environment(0, 3, 3)) == request(6)
    assert strategy.assess(mock_environment(0, 4, 4)) == request(8)


def test_more_instances_are_requested_when_below_minimum():
    strategy = SimpleStrategy(service=service, minimum=2)

    assert strategy.assess({}) == request(2)
    assert strategy.assess(mock_environment(0, 0, 0)) == request(2)
    assert strategy.assess(mock_environment(1, 0, 0)) == request(2)
    assert strategy.assess(mock_environment(2, 0, 0)) == request(2)
    assert strategy.assess(mock_environment(3, 0, 0)) == request(2)
    assert strategy.assess(mock_environment(4, 0, 0)) == request(2)
    assert strategy.assess(mock_environment(1, 1, 0)) == request(2)
    assert strategy.assess(mock_environment(2, 2, 0)) == request(2)
    assert strategy.assess(mock_environment(3, 3, 0)) == request(3)
    assert strategy.assess(mock_environment(4, 4, 0)) == request(4)
    assert strategy.assess(mock_environment(0, 1, 0)) == request(2)
    assert strategy.assess(mock_environment(0, 2, 0)) == request(2)
    assert strategy.assess(mock_environment(0, 3, 0)) == request(3)
    assert strategy.assess(mock_environment(0, 4, 0)) == request(4)
    assert strategy.assess(mock_environment(0, 1, 1)) == request(2)
    assert strategy.assess(mock_environment(0, 2, 2)) == request(4)
    assert strategy.assess(mock_environment(0, 3, 3)) == request(6)
    assert strategy.assess(mock_environment(0, 4, 4)) == request(8)


def test_fewer_instances_are_requested_when_above_maximum():
    strategy = SimpleStrategy(service=service, maximum=2)

    assert strategy.assess({}) == request(0)
    assert strategy.assess(mock_environment(0, 0, 0)) == request(0)
    assert strategy.assess(mock_environment(1, 0, 0)) == request(0)
    assert strategy.assess(mock_environment(2, 0, 0)) == request(0)
    assert strategy.assess(mock_environment(3, 0, 0)) == request(0)
    assert strategy.assess(mock_environment(4, 0, 0)) == request(0)
    assert strategy.assess(mock_environment(1, 1, 0)) == request(1)
    assert strategy.assess(mock_environment(2, 2, 0)) == request(2)
    assert strategy.assess(mock_environment(3, 3, 0)) == request(2)
    assert strategy.assess(mock_environment(4, 4, 0)) == request(2)
    assert strategy.assess(mock_environment(0, 1, 0)) == request(1)
    assert strategy.assess(mock_environment(0, 2, 0)) == request(2)
    assert strategy.assess(mock_environment(0, 3, 0)) == request(2)
    assert strategy.assess(mock_environment(0, 4, 0)) == request(2)
    assert strategy.assess(mock_environment(0, 1, 1)) == request(2)
    assert strategy.assess(mock_environment(0, 2, 2)) == request(2)
    assert strategy.assess(mock_environment(0, 3, 3)) == request(2)
    assert strategy.assess(mock_environment(0, 4, 4)) == request(2)


def test_adjust_instances_when_outside_boundaries():
    strategy = SimpleStrategy(service=service, minimum=2, maximum=3)

    assert strategy.assess({}) == request(2)
    assert strategy.assess(mock_environment(0, 0, 0)) == request(2)
    assert strategy.assess(mock_environment(1, 0, 0)) == request(2)
    assert strategy.assess(mock_environment(2, 0, 0)) == request(2)
    assert strategy.assess(mock_environment(3, 0, 0)) == request(2)
    assert strategy.assess(mock_environment(4, 0, 0)) == request(2)
    assert strategy.assess(mock_environment(1, 1, 0)) == request(2)
    assert strategy.assess(mock_environment(2, 2, 0)) == request(2)
    assert strategy.assess(mock_environment(3, 3, 0)) == request(3)
    assert strategy.assess(mock_environment(4, 4, 0)) == request(3)
    assert strategy.assess(mock_environment(0, 1, 0)) == request(2)
    assert strategy.assess(mock_environment(0, 2, 0)) == request(2)
    assert strategy.assess(mock_environment(0, 3, 0)) == request(3)
    assert strategy.assess(mock_environment(0, 4, 0)) == request(3)
    assert strategy.assess(mock_environment(0, 1, 1)) == request(2)
    assert strategy.assess(mock_environment(0, 2, 2)) == request(3)
    assert strategy.assess(mock_environment(0, 3, 3)) == request(3)
    assert strategy.assess(mock_environment(0, 4, 4)) == request(3)
