from __future__ import annotations

from itertools import count
from unittest import mock

import pytest

from dlstbx.controller.strategyenvironment import StrategyEnvironment


@mock.patch(
    "dlstbx.controller.strategyenvironment.dlstbx.controller.strategy.simple.SimpleStrategy"
)
def test_can_define_set_of_strategies_ignoring_errors(st):
    se = StrategyEnvironment()

    assert se.strategies == {}

    st.side_effect = (
        mock.sentinel.rv1,
        mock.sentinel.rv2,
        mock.sentinel.rv3,
        RuntimeError,
    )

    se.update_strategies(
        [
            {
                "strategy": "simple",
                "service": "A",
                "minimum": mock.sentinel.minimum,
                "maximum": mock.sentinel.maximum,
            },
            {
                "strategy": "simple",
                "service": "B",
                "minimum": mock.sentinel.minimum,
                "maximum": mock.sentinel.maximum,
            },
        ]
    )

    st.assert_has_calls(
        [
            mock.call(
                service="A",
                strategy="simple",
                minimum=mock.sentinel.minimum,
                maximum=mock.sentinel.maximum,
            ),
            mock.call(
                service="B",
                strategy="simple",
                minimum=mock.sentinel.minimum,
                maximum=mock.sentinel.maximum,
            ),
        ]
    )

    assert se.strategies == {"A": mock.sentinel.rv1, "B": mock.sentinel.rv2}

    se.update_strategies([{"strategy": "simple", "service": "B"}])

    st.assert_called_with(service="B", strategy="simple")

    assert se.strategies == {"B": mock.sentinel.rv3}

    with pytest.raises(RuntimeError):
        se.update_strategies([{"strategy": "simple", "service": "Dummy"}])

    assert se.strategies == {"B": mock.sentinel.rv3}


@mock.patch("dlstbx.controller.strategyenvironment.uuid")
def test_that_assessments_are_run_and_services_and_instances_are_created(uu):
    uu.uuid4 = lambda c=count(): next(c)

    se = StrategyEnvironment()
    se.update_strategies(
        [
            {"strategy": "simple", "service": "A", "minimum": 2, "maximum": 4},
            {"strategy": "simple", "service": "B", "minimum": 2, "maximum": 4},
        ]
    )
    assert se.assessments == {}

    se.update_allocation()

    assert se.environment["services"] == {
        "A": {
            "0": {
                "first-seen": mock.ANY,
                "last-seen": mock.ANY,
                "status": se.S_HOLD,
                "tag": "0",
                "service": "A",
                "status-set": mock.ANY,
            },
            "1": {
                "first-seen": mock.ANY,
                "last-seen": mock.ANY,
                "status": se.S_HOLD,
                "tag": "1",
                "service": "A",
                "status-set": mock.ANY,
            },
        },
        "B": {
            "2": {
                "first-seen": mock.ANY,
                "last-seen": mock.ANY,
                "status": se.S_HOLD,
                "tag": "2",
                "service": "B",
                "status-set": mock.ANY,
            },
            "3": {
                "first-seen": mock.ANY,
                "last-seen": mock.ANY,
                "status": se.S_HOLD,
                "tag": "3",
                "service": "B",
                "status-set": mock.ANY,
            },
        },
    }

    assert se.environment["instances"] == {
        "0": {
            "first-seen": mock.ANY,
            "last-seen": mock.ANY,
            "status": se.S_HOLD,
            "tag": "0",
            "service": "A",
            "status-set": mock.ANY,
        },
        "1": {
            "first-seen": mock.ANY,
            "last-seen": mock.ANY,
            "status": se.S_HOLD,
            "tag": "1",
            "service": "A",
            "status-set": mock.ANY,
        },
        "2": {
            "first-seen": mock.ANY,
            "last-seen": mock.ANY,
            "status": se.S_HOLD,
            "tag": "2",
            "service": "B",
            "status-set": mock.ANY,
        },
        "3": {
            "first-seen": mock.ANY,
            "last-seen": mock.ANY,
            "status": se.S_HOLD,
            "tag": "3",
            "service": "B",
            "status-set": mock.ANY,
        },
    }


def generate_test_strategy_environment(allocation=None):
    if not allocation:
        allocation = [2, 2, 2, 2, 2, 2, 2]
    se = StrategyEnvironment()
    for status, num in zip(range(se.S_STATUS_CODE_RANGE), allocation):
        for n in range(num):
            inst = se.create_instance("X", status=status)
            inst["original_status"] = status
    return se


def test_service_instances_are_allocated_correctly():
    expected_allocation_changes = [
        #  -- prior allocation -  req.  -- post allocation --
        #   HD PR ST RN HS SD XP  inst   HD PR ST RN HS SD XP
        ([2, 2, 2, 2, 2, 2, 2], 0, [0, 0, 2, 0, 4, 2, 2]),
        ([2, 2, 2, 2, 2, 2, 2], 1, [0, 0, 2, 1, 3, 2, 2]),
        ([2, 2, 2, 2, 2, 2, 2], 2, [0, 0, 2, 2, 2, 2, 2]),
        ([2, 2, 2, 2, 2, 2, 2], 3, [0, 0, 2, 2, 2, 2, 2]),
        ([2, 2, 2, 2, 2, 2, 2], 4, [0, 0, 2, 2, 2, 2, 2]),
        ([2, 2, 2, 2, 2, 2, 2], 5, [0, 0, 2, 3, 1, 2, 2]),
        ([2, 2, 2, 2, 2, 2, 2], 6, [0, 0, 2, 4, 0, 2, 2]),
        ([2, 2, 2, 2, 2, 2, 2], 7, [0, 0, 2, 5, 0, 1, 2]),
        ([2, 2, 2, 2, 2, 2, 2], 8, [0, 0, 2, 6, 0, 0, 2]),
        ([2, 2, 2, 2, 2, 2, 2], 9, [0, 1, 2, 6, 0, 0, 2]),
        ([2, 2, 2, 2, 2, 2, 2], 10, [0, 2, 2, 6, 0, 0, 2]),
        ([2, 2, 2, 2, 2, 2, 2], 11, [1, 2, 2, 6, 0, 0, 2]),
        ([2, 2, 2, 2, 2, 2, 2], 12, [2, 2, 2, 6, 0, 0, 2]),
        ([2, 2, 2, 2, 2, 2, 2], 13, [3, 2, 2, 6, 0, 0, 2]),
        ([0, 0, 0, 2, 0, 0, 1], 2, [0, 0, 0, 2, 0, 0, 1]),
    ]

    for prior, req_inst, allocation in expected_allocation_changes:
        print(
            "Testing allocation for %d required instances given %s"
            % (req_inst, str(prior))
        )
        se = generate_test_strategy_environment(prior)
        assessment = mock.Mock()
        assessment.assess.return_value = {"required": {"count": req_inst}}
        se.strategies = {"X": assessment}

        se.update_allocation()

        assert len(se.environment["instances"]) == len(se.environment["services"]["X"])
        status_count = [0] * se.S_STATUS_CODE_RANGE
        for instance in se.environment["instances"]:
            status_count[se.environment["instances"][instance]["status"]] += 1
        assert status_count == allocation


def test_ordering_service_instances_prefers_newer_workflows_versions():
    instances = [
        {"workflows": [0, 35]},
        {"workflows": [0, 27]},
        {"workflows": [0, 40]},
        {"workflows": [0, 30]},
        {},
    ]
    se = generate_test_strategy_environment()
    assert [instances[k] for k in (4, 1, 3, 0, 2)] == se.order_instances(instances)
    assert [instances[k] for k in (2, 0, 3, 1, 4)] == se.order_instances(
        instances, reverse=True
    )


def test_ordering_service_instances_prefers_newer_dlstbx_versions_when_same_workflows_version():
    instances = [
        {"dlstbx": [0, 30]},
        {"dlstbx": [0, 17]},
        {"dlstbx": [0, 40]},
        {"dlstbx": [0, 23]},
        {"dlstbx": [0, 23], "workflows": [0, 10]},
        {},
    ]
    se = generate_test_strategy_environment()
    assert [instances[k] for k in (5, 1, 3, 0, 2, 4)] == se.order_instances(instances)
    assert [instances[k] for k in (4, 2, 0, 3, 1, 5)] == se.order_instances(
        instances, reverse=True
    )


def test_ordering_service_instances_prefers_toolserver_instances():
    instances = [
        {"dlstbx": [0, 30], "host": "somewhere.diamond.ac.uk"},
        {"dlstbx": [0, 17], "host": "cs04r-sc-vserv-123.diamond.ac.uk"},
        {"dlstbx": [0, 17]},
        {"dlstbx": [0, 30], "host": "cs04r-sc-vserv-123.diamond.ac.uk"},
        {},
    ]
    se = generate_test_strategy_environment()
    assert [instances[k] for k in (4, 2, 1, 0, 3)] == se.order_instances(instances)
    assert [instances[k] for k in (3, 0, 1, 2, 4)] == se.order_instances(
        instances, reverse=True
    )
