from __future__ import annotations

import functools

import zocalo.configuration
from dlstbx.mimas import MimasDCClass, MimasEvent, MimasScenario, handle_scenario


def test_dummy_plugin(mocker, with_dummy_plugins):
    scenario = functools.partial(
        MimasScenario,
        DCID=123456,
        beamline="i99",
        visit="cm12345-6",
        event=MimasEvent.END,
        runstatus="happy",
    )

    mock_zc = mocker.MagicMock(zocalo.configuration.Configuration, autospec=True)
    invocations = handle_scenario(scenario(dcclass=MimasDCClass.GRIDSCAN), zc=mock_zc)
    assert len(invocations) == 1
    assert {r.recipe for r in invocations} == {"spam"}

    invocations = handle_scenario(scenario(dcclass=MimasDCClass.ROTATION), zc=mock_zc)
    assert len(invocations) == 3
    assert {r.recipe for r in invocations} == {"spam", "foo", "bar"}
