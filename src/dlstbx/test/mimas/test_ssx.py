from __future__ import annotations

import functools

import pytest

from dlstbx.mimas import MimasDCClass, MimasDetectorClass, MimasEvent, MimasScenario
from dlstbx.test.mimas.test_core import get_zocalo_commands


@pytest.mark.parametrize(
    "dcclass", [MimasDCClass.SERIAL_FIXED, MimasDCClass.SERIAL_JET]
)
def test_pilatus_ssx(dcclass):
    dcid = 8374193
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=dcclass,
        beamline="i24",
        visit="nt27314-41",
        runstatus="DataCollection Successful",
        detectorclass=MimasDetectorClass.PILATUS,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == set()
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-ssx-pilatus --trigger",
    }
