import functools

import pytest

import dlstbx.mimas.core
from dlstbx.mimas import (
    MimasDCClass,
    MimasDetectorClass,
    MimasEvent,
    MimasISPyBAnomalousScatterer,
    MimasISPyBSpaceGroup,
    MimasISPyBSweep,
    MimasScenario,
)


def get_zocalo_commands(scenario):
    commands = set()
    actions = dlstbx.mimas.core.run(scenario)
    for a in actions:
        dlstbx.mimas.validate(a)
        commands.add(dlstbx.mimas.zocalo_command_line(a).strip())
    return commands


@pytest.mark.parametrize(
    "anomalous_scatterer,absorption_level", [("S", "high"), (None, "medium")]
)
def test_eiger_rotation(anomalous_scatterer, absorption_level):
    dcid = 5918093
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.ROTATION,
        beamline="i24",
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
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii-eiger --add-param=resolution.cc_half_significance_level:0.1",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp-eiger --trigger",
        f"zocalo.go -r generate-diffraction-preview {dcid}",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials-eiger --add-param=resolution.cc_half_significance_level:0.1 --add-param=absorption_level:{absorption_level} --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC-eiger",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r archive-nexus {dcid}",
    }


@pytest.mark.parametrize(
    "anomalous_scatterer,absorption_level", [("Se", "high"), (None, "medium")]
)
def test_eiger_rotation_multixia2(anomalous_scatterer, absorption_level):
    dcid = 6123722
    other_dcid = 6123719
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.ROTATION,
        beamline="i04",
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
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC-eiger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp-eiger --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-multi-xia2-3dii-eiger --add-sweep={other_dcid}:1:3600 --add-sweep={dcid}:1:3600",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-multi-xia2-dials-eiger --add-sweep={other_dcid}:1:3600 --add-sweep={dcid}:1:3600 --add-param=absorption_level:{absorption_level}",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii-eiger --add-param=resolution.cc_half_significance_level:0.1",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials-eiger --add-param=resolution.cc_half_significance_level:0.1 --add-param=absorption_level:{absorption_level} --trigger",
        f"zocalo.go -r archive-nexus {dcid}",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r generate-diffraction-preview {dcid}",
    }


def test_eiger_screening():
    dcid = 6017522
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.SCREENING,
        beamline="i03",
        runstatus="DataCollection Successful",
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.EIGER,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == {
        f"zocalo.go -r per-image-analysis-rotation-swmr {dcid}"
    }
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        f"zocalo.go -r archive-nexus {dcid}",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r generate-diffraction-preview {dcid}",
        f"zocalo.go -r strategy-align-crystal {dcid}",
        f"zocalo.go -r strategy-edna-eiger {dcid}",
        f"zocalo.go -r strategy-mosflm {dcid}",
    }
    return


def test_eiger_gridscan():
    dcid = 6138194
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.GRIDSCAN,
        beamline="i24",
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


def test_cbf_screening():
    dcid = 5944880
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.SCREENING,
        beamline="i04-1",
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
def test_cbf_rotation(anomalous_scatterer, absorption_level):
    dcid = 5881028
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.ROTATION,
        beamline="i04-1",
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
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii --add-param=resolution.cc_half_significance_level:0.1",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials --add-param=resolution.cc_half_significance_level:0.1 --add-param=absorption_level:{absorption_level} --trigger",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r processing-rlv {dcid}",
    }


@pytest.mark.parametrize(
    "anomalous_scatterer,absorption_level", [("Se", "high"), (None, "medium")]
)
def test_cbf_rotation_multixia2(anomalous_scatterer, absorption_level):
    dcid = 1234567
    other_dcid = 1234566
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.ROTATION,
        beamline="i04-1",
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
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-multi-xia2-dials --add-sweep={other_dcid}:1:3600 --add-sweep={dcid}:1:3600 --add-param=resolution.cc_half_significance_level:0.1 --add-param=absorption_level:{absorption_level}",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii --add-param=resolution.cc_half_significance_level:0.1",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials --add-param=resolution.cc_half_significance_level:0.1 --add-param=absorption_level:{absorption_level} --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-multi-xia2-3dii --add-sweep={other_dcid}:1:3600 --add-sweep={dcid}:1:3600 --add-param=resolution.cc_half_significance_level:0.1",
        f"zocalo.go -r processing-rlv {dcid}",
    }


@pytest.mark.parametrize(
    "anomalous_scatterer,absorption_level", [("Se", "high"), (None, "medium")]
)
def test_cbf_rotation_with_spacegroup(anomalous_scatterer, absorption_level):
    dcid = 6061343
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.ROTATION,
        beamline="i24",
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
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC --add-param=spacegroup:P43212",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp --add-param=spacegroup:P43212 --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii --add-param=resolution.cc_half_significance_level:0.1",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii --add-param=resolution.cc_half_significance_level:0.1 --add-param=spacegroup:P43212",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials --add-param=resolution.cc_half_significance_level:0.1 --add-param=absorption_level:{absorption_level} --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials --add-param=resolution.cc_half_significance_level:0.1 --add-param=spacegroup:P43212 --add-param=absorption_level:{absorption_level} --trigger",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r processing-rlv {dcid}",
    }


def test_cbf_gridscan():
    dcid = 5899304
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.GRIDSCAN,
        event=MimasEvent.START,
        beamline="i04-1",
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


def test_vmxi_gridscan():
    dcid = 5790074
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.GRIDSCAN,
        event=MimasEvent.START,
        beamline="i02-2",
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
def test_vmxi_rotation(anomalous_scatterer, absorption_level):
    dcid = 5590481
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.ROTATION,
        event=MimasEvent.START,
        beamline="i02-2",
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
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC-eiger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp-eiger --trigger",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii-eiger --add-param=resolution.cc_half_significance_level:0.1",
        f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials-eiger --add-param=resolution.cc_half_significance_level:0.1 --add-param=remove_blanks:true --add-param=failover:true --add-param=absorption_level:{absorption_level} --trigger",
        f"zocalo.go -r archive-nexus {dcid}",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r generate-diffraction-preview {dcid}",
        f"zocalo.go -r vmxi-per-image-analysis {dcid}",
    }


@pytest.mark.parametrize(
    "symmetry, symmetry_params",
    [
        ({}, ()),
        (
            {
                "spacegroup": dlstbx.mimas.MimasISPyBSpaceGroup("P21"),
                "unitcell": dlstbx.mimas.MimasISPyBUnitCell(
                    10.89, 8.69, 7.77, 90.0, 103.0, 90.0
                ),
            },
            (
                "--add-param=spacegroup:P1211",
                "--add-param=unit_cell:10.89,8.69,7.77,90.0,103.0,90.0",
            ),
        ),
    ],
    ids=("rotation", "rotation_known_symmetry"),
)
def test_i19(symmetry, symmetry_params):
    """Test the I19 scenario, first without and then with specified crystal symmetry."""
    dcid = 6356546
    other_dcid = 6356585
    scenario = functools.partial(
        MimasScenario,
        DCID=dcid,
        dcclass=MimasDCClass.ROTATION,
        event=MimasEvent.START,
        beamline="i19-1",
        runstatus="DataCollection Successful",
        getsweepslistfromsamedcg=(
            MimasISPyBSweep(DCID=dcid, start=1, end=850),
            MimasISPyBSweep(DCID=other_dcid, start=1, end=850),
        ),
        preferred_processing="xia2/DIALS",
        detectorclass=MimasDetectorClass.PILATUS,
        **symmetry,
    )
    assert get_zocalo_commands(scenario(event=MimasEvent.START)) == {
        f"zocalo.go -r per-image-analysis-rotation {dcid}",
    }
    assert get_zocalo_commands(scenario(event=MimasEvent.END)) == {
        " ".join(
            (
                "ispyb.job",
                "--new",
                f"--dcid={dcid}",
                "--source=automatic",
                "--recipe=autoprocessing-multi-xia2-smallmolecule",
                f"--add-sweep={dcid}:1:850",
                f"--add-sweep={other_dcid}:1:850",
                *symmetry_params,
                "--add-param=absorption_level:medium",
                "--trigger",
            )
        ),
        " ".join(
            (
                "ispyb.job",
                "--new",
                f"--dcid={dcid}",
                "--source=automatic",
                "--recipe=autoprocessing-multi-xia2-smallmolecule",
                f"--add-sweep={dcid}:1:850",
                f"--add-sweep={other_dcid}:1:850",
                "--add-param=absorption_level:medium",
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
                f"--add-sweep={dcid}:1:850",
                f"--add-sweep={other_dcid}:1:850",
                *symmetry_params,
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
                f"--add-sweep={dcid}:1:850",
                f"--add-sweep={other_dcid}:1:850",
                "--trigger",
            )
        ),
        f"zocalo.go -r archive-cbfs {dcid}",
        f"zocalo.go -r generate-crystal-thumbnails {dcid}",
        f"zocalo.go -r processing-rlv {dcid}",
        f"zocalo.go -r strategy-screen19 {dcid}",
    }
