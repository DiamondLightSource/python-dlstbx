import functools
from typing import List

import pkg_resources
import pytest

from dlstbx.mimas import (
    Invocation,
    MimasDCClass,
    MimasEvent,
    MimasRecipeInvocation,
    MimasScenario,
    handle_scenario,
    match_specification,
)
from dlstbx.mimas.specification import BeamlineSpecification, DCClassSpecification

is_i99 = BeamlineSpecification("i99")
is_rotation = DCClassSpecification(MimasDCClass.ROTATION)


@match_specification(is_i99 & is_rotation)
def handle_i99_rotation(scenario: MimasScenario) -> List[Invocation]:
    return [
        MimasRecipeInvocation(DCID=scenario.DCID, recipe="foo"),
        MimasRecipeInvocation(DCID=scenario.DCID, recipe="bar"),
    ]


@match_specification(is_i99)
def handle_i99(scenario: MimasScenario) -> List[Invocation]:
    return [
        MimasRecipeInvocation(DCID=scenario.DCID, recipe="spam"),
    ]


@pytest.fixture
def with_dummy_plugins():
    # Get the current distribution and entry map
    dist = pkg_resources.get_distribution("dlstbx")
    entry_map = pkg_resources.get_entry_map("dlstbx", group="zocalo.mimas.handlers")

    # Create the fake entry point definitions and add the mapping
    entry_map["i99"] = pkg_resources.EntryPoint.parse(
        f"i99 = {__name__}:handle_i99", dist=dist
    )
    entry_map["i99_rotation"] = pkg_resources.EntryPoint.parse(
        f"i99_rotation = {__name__}:handle_i99_rotation", dist=dist
    )
    yield
    # cleanup
    del entry_map["i99"]
    del entry_map["i99_rotation"]


def test_dummy_plugin(with_dummy_plugins):
    scenario = functools.partial(
        MimasScenario,
        DCID=123456,
        beamline="i99",
        visit="cm12345-6",
        event=MimasEvent.END,
        runstatus="happy",
    )

    invocations = handle_scenario(scenario(dcclass=MimasDCClass.GRIDSCAN))
    assert len(invocations) == 1
    assert {r.recipe for r in invocations} == {"spam"}

    invocations = handle_scenario(scenario(dcclass=MimasDCClass.ROTATION))
    assert len(invocations) == 3
    assert {r.recipe for r in invocations} == {"spam", "foo", "bar"}
