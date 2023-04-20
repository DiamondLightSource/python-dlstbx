from __future__ import annotations

import functools

import pytest

from dlstbx import mimas
from dlstbx.mimas import (
    MimasDCClass,
    MimasDetectorClass,
    MimasEvent,
    MimasISPyBAnomalousScatterer,
    MimasISPyBSpaceGroup,
    MimasISPyBSweep,
    MimasScenario,
)

dac_dials_params = (
    "--add-param=dynamic_shadowing:true",
    "--add-param=ice_rings.filter:true",
    "--add-param=ice_rings.unit_cell:3.1652,3.1652,3.1652,90,90,90",
    "--add-param=ice_rings.space_group:Im-3m",
    "--add-param=ice_rings.width:0.01",
    "--add-param=scan_varying:true",
    "--add-param=resolution_range:999,15",
    "--add-param=keep_all_reflections:false",
    "--add-param=cc_half:none",
    "--add-param=isigma:2",
)


@pytest.mark.parametrize(
    "anomalous_scatterer,absorption_level", [("S", "high"), (None, "medium")]
)
def test_eiger_rotation(anomalous_scatterer, absorption_level, get_zocalo_commands):
    dcid = 5918093
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.ROTATION,
        beamline="i24",
        visit="nt28218-10",
        runstatus="DataCollection Successful",
        getsweepslistfromsamedcg=(MimasISPyBSweep(DCID=dcid, start=1, end=1000),),
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.EIGER,
        anomalous_scatterer=MimasISPyBAnomalousScatterer
        if anomalous_scatterer
        else None,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == {
        f"zocalo.go -r per-image-analysis-rotation-swmr {dcid}",
    }
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"zocalo.go -r processing-rlv-eiger {dcid}",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii-eiger-cluster --add-param=resolution.cc_half_significance_level:0.1 --display='xia2 3dii'",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp-eiger --display='fast_dp' --trigger",
        f"zocalo.go -r generate-diffraction-preview {dcid}",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials-eiger-cluster --add-param=resolution.cc_half_significance_level:0.1 --add-param=absorption_level:{absorption_level} --display='xia2 dials' --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC-eiger-cluster --display='autoPROC'",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r archive-nexus {dcid}",
    }


@pytest.mark.parametrize(
    "anomalous_scatterer,absorption_level", [("Se", "high"), (None, "medium")]
)
def test_eiger_rotation_multixia2(
    anomalous_scatterer, absorption_level, get_zocalo_commands
):
    dcid = 6123722
    other_dcid = 6123719
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.ROTATION,
        beamline="i04",
        visit="nt28218-5",
        runstatus="DataCollection Successful",
        getsweepslistfromsamedcg=(
            MimasISPyBSweep(DCID=other_dcid, start=1, end=3600),
            MimasISPyBSweep(DCID=dcid, start=1, end=3600),
        ),
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.EIGER,
        anomalous_scatterer=MimasISPyBAnomalousScatterer
        if anomalous_scatterer
        else None,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == {
        f"zocalo.go -r per-image-analysis-rotation-swmr {dcid}",
    }
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"zocalo.go -r processing-rlv-eiger {dcid}",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC-eiger-cluster --display='autoPROC'",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp-eiger --display='fast_dp' --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-multi-xia2-3dii-eiger-cluster --add-sweep={other_dcid}:1:3600 --add-sweep={dcid}:1:3600 --add-param=resolution.cc_half_significance_level:0.1 --display='xia2 3dii (multi)'",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-multi-xia2-dials-eiger-cluster --add-sweep={other_dcid}:1:3600 --add-sweep={dcid}:1:3600 --add-param=resolution.cc_half_significance_level:0.1 --add-param=absorption_level:{absorption_level} --display='xia2 dials (multi)'",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii-eiger-cluster --add-param=resolution.cc_half_significance_level:0.1 --display='xia2 3dii'",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials-eiger-cluster --add-param=resolution.cc_half_significance_level:0.1 --add-param=absorption_level:{absorption_level} --display='xia2 dials' --trigger",
        f"zocalo.go -r archive-nexus {dcid}",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r generate-diffraction-preview {dcid}",
    }


def test_eiger_screening(get_zocalo_commands):
    dcid = 6017522
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.SCREENING,
        beamline="i03",
        visit="nt28218-3",
        runstatus="DataCollection Successful",
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.EIGER,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == set()
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"zocalo.go -r archive-nexus {dcid}",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r generate-diffraction-preview {dcid}",
        f"zocalo.go -r per-image-analysis-rotation-swmr {dcid}",
        f"zocalo.go -r strategy-align-crystal {dcid}",
        f"zocalo.go -r strategy-edna-eiger {dcid}",
        f"zocalo.go -r strategy-mosflm {dcid}",
    }
    return


def test_eiger_gridscan(get_zocalo_commands):
    dcid = 6138194
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.GRIDSCAN,
        beamline="i24",
        visit="nt28218-10",
        runstatus="DataCollection Successful",
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.EIGER,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == {
        f"zocalo.go -r per-image-analysis-gridscan-swmr {dcid}"
    }
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"zocalo.go -r archive-nexus {dcid}",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r generate-diffraction-preview {dcid}",
    }


def test_cbf_screening(get_zocalo_commands):
    dcid = 5944880
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.SCREENING,
        beamline="i04-1",
        visit="nt28218-6",
        runstatus="DataCollection Successful",
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.PILATUS,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == {
        f"zocalo.go -r archive-cbfs {dcid}",
        f"zocalo.go -r per-image-analysis-rotation {dcid}",
    }
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r strategy-edna {dcid}",
        f"zocalo.go -r strategy-mosflm {dcid}",
    }


@pytest.mark.parametrize(
    "anomalous_scatterer,absorption_level", [("Se", "high"), (None, "medium")]
)
def test_cbf_rotation(anomalous_scatterer, absorption_level, get_zocalo_commands):
    dcid = 5881028
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.ROTATION,
        beamline="i04-1",
        visit="nt28218-6",
        runstatus="DataCollection Successful",
        getsweepslistfromsamedcg=(MimasISPyBSweep(DCID=dcid, start=1, end=375),),
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.PILATUS,
        anomalous_scatterer=MimasISPyBAnomalousScatterer
        if anomalous_scatterer
        else None,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == {
        f"zocalo.go -r archive-cbfs {dcid}",
        f"zocalo.go -r per-image-analysis-rotation {dcid}",
    }
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC --display='autoPROC'",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp --display='fast_dp' --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii --add-param=resolution.cc_half_significance_level:0.1 --display='xia2 3dii'",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials --add-param=resolution.cc_half_significance_level:0.1 --add-param=absorption_level:{absorption_level} --display='xia2 dials' --trigger",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r processing-rlv {dcid}",
    }


@pytest.mark.parametrize(
    "anomalous_scatterer,absorption_level", [("Se", "high"), (None, "medium")]
)
def test_cbf_rotation_multixia2(
    anomalous_scatterer, absorption_level, get_zocalo_commands
):
    dcid = 1234567
    other_dcid = 1234566
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.ROTATION,
        beamline="i04-1",
        visit="nt28218-6",
        runstatus="DataCollection Successful",
        getsweepslistfromsamedcg=(
            MimasISPyBSweep(DCID=other_dcid, start=1, end=3600),
            MimasISPyBSweep(DCID=dcid, start=1, end=3600),
        ),
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.PILATUS,
        anomalous_scatterer=MimasISPyBAnomalousScatterer
        if anomalous_scatterer
        else None,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == {
        f"zocalo.go -r archive-cbfs {dcid}",
        f"zocalo.go -r per-image-analysis-rotation {dcid}",
    }
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp --display='fast_dp' --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-multi-xia2-dials --add-sweep={other_dcid}:1:3600 --add-sweep={dcid}:1:3600 --add-param=resolution.cc_half_significance_level:0.1 --add-param=absorption_level:{absorption_level} --display='xia2 dials (multi)'",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii --add-param=resolution.cc_half_significance_level:0.1 --display='xia2 3dii'",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials --add-param=resolution.cc_half_significance_level:0.1 --add-param=absorption_level:{absorption_level} --display='xia2 dials' --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC --display='autoPROC'",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-multi-xia2-3dii --add-sweep={other_dcid}:1:3600 --add-sweep={dcid}:1:3600 --add-param=resolution.cc_half_significance_level:0.1 --display='xia2 3dii (multi)'",
        f"zocalo.go -r processing-rlv {dcid}",
    }


@pytest.mark.parametrize(
    "anomalous_scatterer,absorption_level", [("Se", "high"), (None, "medium")]
)
def test_cbf_rotation_with_spacegroup(
    anomalous_scatterer, absorption_level, get_zocalo_commands
):
    dcid = 6061343
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.ROTATION,
        beamline="i24",
        visit="nt28218-10",
        runstatus="DataCollection Successful",
        spacegroup=MimasISPyBSpaceGroup(symbol="P43212"),
        getsweepslistfromsamedcg=(MimasISPyBSweep(DCID=dcid, start=1, end=3600),),
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.PILATUS,
        anomalous_scatterer=MimasISPyBAnomalousScatterer
        if anomalous_scatterer
        else None,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == {
        f"zocalo.go -r archive-cbfs {dcid}",
        f"zocalo.go -r per-image-analysis-rotation {dcid}",
    }
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC --display='autoPROC'",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC --add-param=spacegroup:P43212 --display='autoPROC'",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp --add-param=spacegroup:P43212 --display='fast_dp' --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii --add-param=resolution.cc_half_significance_level:0.1 --display='xia2 3dii'",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii --add-param=resolution.cc_half_significance_level:0.1 --add-param=spacegroup:P43212 --display='xia2 3dii'",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials --add-param=resolution.cc_half_significance_level:0.1 --add-param=absorption_level:{absorption_level} --display='xia2 dials' --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials --add-param=resolution.cc_half_significance_level:0.1 --add-param=spacegroup:P43212 --add-param=absorption_level:{absorption_level} --display='xia2 dials' --trigger",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r processing-rlv {dcid}",
    }


def test_cbf_gridscan(get_zocalo_commands):
    dcid = 5899304
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.GRIDSCAN,
        event=MimasEvent.START,
        beamline="i04-1",
        visit="nt28218-6",
        runstatus="DataCollection Successful",
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.PILATUS,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == {
        f"zocalo.go -r archive-cbfs {dcid}",
        f"zocalo.go -r per-image-analysis-gridscan {dcid}",
    }
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
    }


def test_vmxi_gridscan(get_zocalo_commands):
    dcid = 5790074
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.GRIDSCAN,
        event=MimasEvent.START,
        beamline="i02-2",
        visit="nt28218-1",
        runstatus="DataCollection Successful",
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.EIGER,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == set()
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"zocalo.go -r archive-nexus {dcid}",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r generate-diffraction-preview {dcid}",
        f"zocalo.go -r vmxi-spot-counts-per-image {dcid}",
    }


@pytest.mark.parametrize(
    "anomalous_scatterer,absorption_level", [("Se", "high"), (None, "medium")]
)
def test_vmxi_rotation(anomalous_scatterer, absorption_level, get_zocalo_commands):
    dcid = 5590481
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.ROTATION,
        event=MimasEvent.START,
        beamline="i02-2",
        visit="nt28218-1",
        runstatus="DataCollection Successful",
        getsweepslistfromsamedcg=(MimasISPyBSweep(DCID=dcid, start=1, end=600),),
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.EIGER,
        anomalous_scatterer=MimasISPyBAnomalousScatterer
        if anomalous_scatterer
        else None,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == set()
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC-eiger --display='autoPROC'",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp-eiger --display='fast_dp' --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii-eiger --add-param=resolution.cc_half_significance_level:0.1 --display='xia2 3dii'",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials-eiger --add-param=resolution.cc_half_significance_level:0.1 --add-param=remove_blanks:true --add-param=failover:true --add-param=absorption_level:{absorption_level} --display='xia2 dials' --trigger",
        f"zocalo.go -r archive-nexus {dcid}",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r generate-diffraction-preview {dcid}",
        f"zocalo.go -r vmxi-per-image-analysis {dcid}",
    }


def test_vmxm_rotation(get_zocalo_commands):
    dcid = 7389381
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.ROTATION,
        event=MimasEvent.START,
        beamline="i02-1",
        visit="nt27314-31",
        runstatus="DataCollection Successful",
        getsweepslistfromsamedcg=(MimasISPyBSweep(DCID=dcid, start=1, end=600),),
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.EIGER,
        anomalous_scatterer=None,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == {
        f"zocalo.go -r per-image-analysis-rotation-swmr-vmxm {dcid}"
    }
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"ispyb.job --new --dcid={dcid} --source=automatic "
        "--recipe=autoprocessing-autoPROC-eiger-cluster --display='autoPROC'",
        f"ispyb.job --new --dcid={dcid} --source=automatic "
        f"--recipe=autoprocessing-fast-dp-eiger --display='fast_dp' --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic "
        "--recipe=autoprocessing-xia2-3dii-eiger-cluster "
        "--add-param=resolution.cc_half_significance_level:0.1 --display='xia2 3dii'",
        f"ispyb.job --new --dcid={dcid} --source=automatic "
        "--recipe=autoprocessing-xia2-dials-eiger-cluster "
        "--add-param=resolution.cc_half_significance_level:0.1 "
        "--add-param=ice_rings.unit_cell:3.615,3.615,3.615,90,90,90 "
        "--add-param=ice_rings.space_group:fm-3m --add-param=ice_rings.width:0.01 "
        "--add-param=ice_rings.filter:true --add-param=remove_blanks:true "
        "--add-param=failover:true --add-param=absorption_level:medium --display='xia2 dials' --trigger",
        f"zocalo.go -r archive-nexus {dcid}",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r generate-diffraction-preview {dcid}",
        f"zocalo.go -r processing-rlv-eiger {dcid}",
    }


def test_vmxm_gridscan(get_zocalo_commands):
    dcid = 7389147
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.GRIDSCAN,
        beamline="i02-1",
        visit="nt27314-31",
        runstatus="DataCollection Successful",
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.EIGER,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == {
        f"zocalo.go -r per-image-analysis-gridscan-swmr-vmxm {dcid}"
    }
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"zocalo.go -r archive-nexus {dcid}",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r generate-diffraction-preview {dcid}",
    }


@pytest.mark.parametrize(
    "detectorclass, pia_type, aimless_string, xia2_type, data_format, rlv_type",
    [
        (MimasDetectorClass.PILATUS, "-i19", "dials-aiml", "", "cbfs", ""),
        (MimasDetectorClass.EIGER, "-swmr-i19", "d-a", "-nexus", "nexus", "-eiger"),
    ],
    ids=("Pilatus", "Eiger"),
)
@pytest.mark.parametrize(
    "dcclass", [MimasDCClass.ROTATION, MimasDCClass.DIAMOND_ANVIL_CELL]
)
def test_i19_rotation(
    detectorclass,
    pia_type,
    aimless_string,
    xia2_type,
    data_format,
    rlv_type,
    dcclass,
    get_zocalo_commands,
):
    """Test the I19 rotation scenario."""
    dcid = 6356546
    other_dcid = 6356585

    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=dcclass,
        event=MimasEvent.START,
        beamline="i19-1",
        visit="nt28218-7",
        runstatus="DataCollection Successful",
        getsweepslistfromsamedcg=(
            MimasISPyBSweep(DCID=dcid, start=1, end=850),
            MimasISPyBSweep(DCID=other_dcid, start=1, end=850),
        ),
        preferred_processing="xia2/DIALS",
        detectorclass=detectorclass,
    )

    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == {
        f"zocalo.go -r per-image-analysis-rotation{pia_type} {dcid}"
    }

    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        " ".join(
            (
                "ispyb.job",
                "--new",
                f"--dcid={dcid}",
                "--source=automatic",
                f"--recipe=autoprocessing-multi-xia2-smallmolecule{xia2_type}",
                f"--add-sweep={dcid}:1:850",
                f"--add-sweep={other_dcid}:1:850",
                "--add-param=absorption_level:medium",
            )
            + (
                dac_dials_params
                if dcclass == mimas.MimasDCClass.DIAMOND_ANVIL_CELL
                else ()
            )
            + (
                "--display='xia2 dials'",
                "--trigger",
            )
        ),
        " ".join(
            (
                "ispyb.job",
                "--new",
                f"--dcid={dcid}",
                "--source=automatic",
                f"--recipe=autoprocessing-multi-xia2-smallmolecule-{aimless_string}{xia2_type}",
                f"--add-sweep={dcid}:1:850",
                f"--add-sweep={other_dcid}:1:850",
            )
            + (
                dac_dials_params
                if dcclass == mimas.MimasDCClass.DIAMOND_ANVIL_CELL
                else ()
            )
            + (
                "--display='xia2 dials-aimless'",
                "--trigger",
            )
        ),
        f"zocalo.go -r archive-{data_format} {dcid}",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r processing-rlv{rlv_type} {dcid}",
        f"zocalo.go -r strategy-screen19{rlv_type} {dcid}",
    }.union(
        {f"zocalo.go -r generate-diffraction-preview {dcid}"}
        if detectorclass is MimasDetectorClass.EIGER
        else set()
    )


@pytest.mark.parametrize(
    "detectorclass, pia_type, aimless_string, xia2_type, data_format, rlv_type",
    [
        (MimasDetectorClass.PILATUS, "-i19", "dials-aiml", "", "cbfs", ""),
        (MimasDetectorClass.EIGER, "-swmr-i19", "d-a", "-nexus", "nexus", "-eiger"),
    ],
    ids=("Pilatus", "Eiger"),
)
@pytest.mark.parametrize(
    "dcclass", [MimasDCClass.ROTATION, MimasDCClass.DIAMOND_ANVIL_CELL]
)
def test_i19_rotation_with_symmetry(
    detectorclass,
    pia_type,
    aimless_string,
    xia2_type,
    data_format,
    rlv_type,
    dcclass,
    get_zocalo_commands,
):
    """Test the I19 rotation scenario with specified crystal symmetry."""
    dcid = 6356546
    other_dcid = 6356585

    spacegroup = mimas.MimasISPyBSpaceGroup("P21")
    unitcell = mimas.MimasISPyBUnitCell(10.89, 8.69, 7.77, 90.0, 103.0, 90.0)

    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=dcclass,
        event=MimasEvent.START,
        beamline="i19-1",
        visit="nt28218-7",
        runstatus="DataCollection Successful",
        getsweepslistfromsamedcg=(
            MimasISPyBSweep(DCID=dcid, start=1, end=850),
            MimasISPyBSweep(DCID=other_dcid, start=1, end=850),
        ),
        preferred_processing="xia2/DIALS",
        detectorclass=detectorclass,
        spacegroup=spacegroup,
        unitcell=unitcell,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == {
        f"zocalo.go -r per-image-analysis-rotation{pia_type} {dcid}"
    }
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        " ".join(
            (
                "ispyb.job",
                "--new",
                f"--dcid={dcid}",
                "--source=automatic",
                f"--recipe=autoprocessing-multi-xia2-smallmolecule{xia2_type}",
                f"--add-sweep={dcid}:1:850",
                f"--add-sweep={other_dcid}:1:850",
                "--add-param=spacegroup:P1211",
                "--add-param=unit_cell:10.89,8.69,7.77,90.0,103.0,90.0",
                "--add-param=absorption_level:medium",
            )
            + (
                dac_dials_params
                if dcclass == mimas.MimasDCClass.DIAMOND_ANVIL_CELL
                else ()
            )
            + (
                "--display='xia2 dials'",
                "--trigger",
            )
        ),
        " ".join(
            (
                "ispyb.job",
                "--new",
                f"--dcid={dcid}",
                "--source=automatic",
                f"--recipe=autoprocessing-multi-xia2-smallmolecule{xia2_type}",
                f"--add-sweep={dcid}:1:850",
                f"--add-sweep={other_dcid}:1:850",
                "--add-param=absorption_level:medium",
            )
            + (
                dac_dials_params
                if dcclass == mimas.MimasDCClass.DIAMOND_ANVIL_CELL
                else ()
            )
            + (
                "--display='xia2 dials'",
                "--trigger",
            )
        ),
        " ".join(
            (
                "ispyb.job",
                "--new",
                f"--dcid={dcid}",
                "--source=automatic",
                f"--recipe=autoprocessing-multi-xia2-smallmolecule-{aimless_string}{xia2_type}",
                f"--add-sweep={dcid}:1:850",
                f"--add-sweep={other_dcid}:1:850",
                "--add-param=spacegroup:P1211",
                "--add-param=unit_cell:10.89,8.69,7.77,90.0,103.0,90.0",
            )
            + (
                dac_dials_params
                if dcclass == mimas.MimasDCClass.DIAMOND_ANVIL_CELL
                else ()
            )
            + (
                "--display='xia2 dials-aimless'",
                "--trigger",
            )
        ),
        " ".join(
            (
                "ispyb.job",
                "--new",
                f"--dcid={dcid}",
                "--source=automatic",
                f"--recipe=autoprocessing-multi-xia2-smallmolecule-{aimless_string}{xia2_type}",
                f"--add-sweep={dcid}:1:850",
                f"--add-sweep={other_dcid}:1:850",
            )
            + (
                dac_dials_params
                if dcclass == mimas.MimasDCClass.DIAMOND_ANVIL_CELL
                else ()
            )
            + (
                "--display='xia2 dials-aimless'",
                "--trigger",
            )
        ),
        f"zocalo.go -r archive-{data_format} {dcid}",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r processing-rlv{rlv_type} {dcid}",
        f"zocalo.go -r strategy-screen19{rlv_type} {dcid}",
    }.union(
        {f"zocalo.go -r generate-diffraction-preview {dcid}"}
        if detectorclass is MimasDetectorClass.EIGER
        else set()
    )


def test_i15_rotation(get_zocalo_commands):
    """Test the I15 rotation scenario."""
    dcid = 8377481
    other_dcid = 8377499

    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.ROTATION,
        event=MimasEvent.START,
        beamline="i15",
        visit="cm31136-2",
        runstatus="DataCollection Successful",
        getsweepslistfromsamedcg=(
            MimasISPyBSweep(DCID=dcid, start=1, end=51),
            MimasISPyBSweep(DCID=other_dcid, start=1, end=51),
        ),
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.PILATUS,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == set()

    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        " ".join(
            (
                "ispyb.job",
                "--new",
                f"--dcid={dcid}",
                "--source=automatic",
                "--recipe=autoprocessing-multi-xia2-smallmolecule",
                f"--add-sweep={dcid}:1:51",
                f"--add-sweep={other_dcid}:1:51",
                "--add-param=absorption_level:medium",
                "--display='xia2 dials'",
                "--trigger",
            )
        ),
        " ".join(
            (
                "ispyb.job",
                "--new",
                f"--dcid={dcid}",
                "--source=automatic",
                "--recipe=autoprocessing-multi-xia2-smallmolecule-dials-aiml",
                f"--add-sweep={dcid}:1:51",
                f"--add-sweep={other_dcid}:1:51",
                "--display='xia2 dials-aimless'",
                "--trigger",
            )
        ),
        f"zocalo.go -r per-image-analysis-rotation {dcid}",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r processing-rlv {dcid}",
        f"zocalo.go -r strategy-screen19 {dcid}",
    }
