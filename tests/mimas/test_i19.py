from __future__ import annotations

import functools

import pytest

from dlstbx.mimas import MimasDCClass, MimasDetectorClass, MimasEvent, MimasScenario


@pytest.mark.xfail
def test_i19_ssx(get_zocalo_commands):
    dcid = 8374193
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.SERIAL_FIXED,
        beamline="i19-2",
        visit="nt27314-41",
        runstatus="DataCollection Successful",
        detectorclass=MimasDetectorClass.EIGER,
    )

    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == set()
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"zocalo.go -r per-image-analysis-rotation-swmr-i19 {dcid}",
        f"zocalo.go -r processing-rlv-eiger {dcid}",
        f"zocalo.go -r generate-diffraction-preview {dcid}",
        f"zocalo.go -r strategy-screen19-eiger {dcid}",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-multi-xia2-smallmolecule-nexus --add-param=absorption_level:medium --display='xia2 dials' --trigger",
    }


def test_i19_serial_group_end(get_zocalo_commands):
    dcid = 8374193
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.SERIAL_FIXED,
        beamline="i19-2",
        visit="nt27314-41",
        runstatus="DataCollection Successful",
        detectorclass=MimasDetectorClass.EIGER,
    )

    assert get_zocalo_commands(scenario(event=MimasEvent.START_GROUP)) == set()
    assert get_zocalo_commands(scenario(event=MimasEvent.END_GROUP)) == {
        f"zocalo.go -r autoprocessing-i19serial-groupend {dcid}",
    }
