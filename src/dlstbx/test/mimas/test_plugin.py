import functools

from dlstbx.mimas import MimasDCClass, MimasEvent, MimasScenario, handle_scenario


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
