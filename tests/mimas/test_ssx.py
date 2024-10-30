from __future__ import annotations

import functools

import pytest

from dlstbx.mimas import MimasDCClass, MimasDetectorClass, MimasEvent, MimasScenario


@pytest.mark.parametrize(
    "dcclass,detectorclass",
    [
        (MimasDCClass.SERIAL_FIXED, MimasDetectorClass.PILATUS),
        (MimasDCClass.SERIAL_JET, MimasDetectorClass.PILATUS),
        (MimasDCClass.SERIAL_FIXED, MimasDetectorClass.EIGER),
        (MimasDCClass.SERIAL_JET, MimasDetectorClass.EIGER),
    ],
)
def test_ssx(dcclass, detectorclass, get_zocalo_commands):
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
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == {
        "zocalo.go -r pia-index-ssx-pilatus 8374193"
    }
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-ssx-pilatus --display='xia2.ssx' --trigger",
    }
