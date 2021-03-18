# flake8: noqa W291
import pytest

import dlstbx.mimas
from dlstbx.cli import mimas


def get_zocalo_commands(dcid):
    scenarios = mimas.get_scenarios(dcid)
    commands = {}
    for scenario in scenarios:
        commands[scenario.event.name] = set()
        actions = dlstbx.mimas.core.run(scenario)
        for a in actions:
            dlstbx.mimas.validate(a)
            commands[scenario.event.name].add(
                dlstbx.mimas.zocalo_command_line(a).strip()
            )
    return commands


@pytest.mark.parametrize(
    "dcid",
    [
        6002672,  # i03
        6123920,  # i04
        5918093,  # i24
    ],
)
def test_eiger_rotation(dcid):
    commands = get_zocalo_commands(dcid)
    print(commands)
    assert commands == {
        "START": {f"zocalo.go -r per-image-analysis-rotation-swmr {dcid}"},
        "END": {
            f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii-eiger  --add-param=resolution.cc_half_significance_level:0.1",
            f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp-eiger   --trigger",
            f"zocalo.go -r generate-diffraction-preview {dcid}",
            f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials-eiger  --add-param=resolution.cc_half_significance_level:0.1 --trigger",
            f"ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC-eiger",
            f"zocalo.go -r generate-crystal-thumbnails {dcid}",
            f"zocalo.go -r archive-nexus {dcid}",
        },
    }


def test_i03_eiger_screening(capsys):
    dcid = 6017522
    mimas.run([f"{dcid}", "-c"])
    captured = capsys.readouterr()
    expected = f"""\
At the start of data collection {dcid}:
 - zocalo.go -r per-image-analysis-rotation-swmr {dcid}

At the end of data collection {dcid}:
 - zocalo.go -r archive-nexus {dcid}
 - zocalo.go -r generate-crystal-thumbnails {dcid}
 - zocalo.go -r generate-diffraction-preview {dcid}
 - zocalo.go -r strategy-align-crystal {dcid}
 - zocalo.go -r strategy-edna-eiger {dcid}
 - zocalo.go -r strategy-mosflm {dcid}"""
    assert expected in captured.out


@pytest.mark.parametrize(
    "dcid",
    [
        6017516,  # i03
        6123908,  # i04
        6138194,  # i24
    ],
)
def test_eiger_gridscan(dcid, capsys):
    mimas.run([f"{dcid}", "-c"])
    captured = capsys.readouterr()
    expected = f"""\
At the start of data collection {dcid}:
 - zocalo.go -r per-image-analysis-gridscan-swmr {dcid}

At the end of data collection {dcid}:
 - zocalo.go -r archive-nexus {dcid}
 - zocalo.go -r generate-crystal-thumbnails {dcid}
 - zocalo.go -r generate-diffraction-preview {dcid}"""
    assert expected in captured.out


@pytest.mark.parametrize(
    "dcid",
    [
        5944880,  # i04-1
    ],
)
def test_cbf_screening(dcid, capsys):
    mimas.run([f"{dcid}", "-c"])
    captured = capsys.readouterr()
    expected = f"""\

At the start of data collection {dcid}:
 - zocalo.go -r archive-cbfs {dcid}
 - zocalo.go -r per-image-analysis-rotation {dcid}

At the end of data collection {dcid}:
 - zocalo.go -r generate-crystal-thumbnails {dcid}
 - zocalo.go -r strategy-edna {dcid}
 - zocalo.go -r strategy-mosflm {dcid}"""
    assert expected in captured.out


@pytest.mark.parametrize(
    "dcid",
    [
        5881028,  # i04-1
    ],
)
def test_cbf_rotation(dcid, capsys):
    mimas.run([f"{dcid}", "-c"])
    captured = capsys.readouterr()
    expected = f"""\
At the start of data collection {dcid}:
 - zocalo.go -r archive-cbfs {dcid}
 - zocalo.go -r per-image-analysis-rotation {dcid}

At the end of data collection {dcid}:
 - ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC   
 - ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp   --trigger 
 - ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii  --add-param=resolution.cc_half_significance_level:0.1 
 - ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials  --add-param=resolution.cc_half_significance_level:0.1 --trigger 
 - zocalo.go -r generate-crystal-thumbnails {dcid}
 - zocalo.go -r processing-rlv {dcid}
"""
    assert expected in captured.out


def test_cbf_rotation_with_spacegroup(capsys):
    dcid = 6061343  # i24
    mimas.run([f"{dcid}", "-c"])
    captured = capsys.readouterr()
    expected = f"""\
At the start of data collection {dcid}:
 - zocalo.go -r archive-cbfs {dcid}
 - zocalo.go -r per-image-analysis-rotation {dcid}

At the end of data collection {dcid}:
 - ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC   
 - ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC  --add-param=spacegroup:P43212 
 - ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp  --add-param=spacegroup:P43212 --trigger 
 - ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii  --add-param=resolution.cc_half_significance_level:0.1 
 - ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii  --add-param=resolution.cc_half_significance_level:0.1 --add-param=spacegroup:P43212 
 - ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials  --add-param=resolution.cc_half_significance_level:0.1 --trigger 
 - ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials  --add-param=resolution.cc_half_significance_level:0.1 --add-param=spacegroup:P43212 --trigger 
 - zocalo.go -r generate-crystal-thumbnails {dcid}
 - zocalo.go -r processing-rlv {dcid}"""
    assert expected in captured.out


@pytest.mark.parametrize(
    "dcid",
    [
        5899304,  # i04-1
        6061232,  # i24
    ],
)
def test_cbf_gridscan(dcid, capsys):
    mimas.run([f"{dcid}", "-c"])
    captured = capsys.readouterr()
    expected = f"""\
At the start of data collection {dcid}:
 - zocalo.go -r archive-cbfs {dcid}
 - zocalo.go -r per-image-analysis-gridscan {dcid}

At the end of data collection {dcid}:
 - zocalo.go -r generate-crystal-thumbnails {dcid}"""
    assert expected in captured.out


def test_vmxi_gridscan(capsys):
    dcid = 5790074
    mimas.run([f"{dcid}", "-c"])
    captured = capsys.readouterr()
    expected = f"""\
At the start of data collection {dcid}:
 - do nothing

At the end of data collection {dcid}:
 - zocalo.go -r archive-nexus {dcid}
 - zocalo.go -r generate-crystal-thumbnails {dcid}
 - zocalo.go -r generate-diffraction-preview {dcid}
 - zocalo.go -r vmxi-spot-counts-per-image {dcid}"""
    assert expected in captured.out


def test_vmxi_rotation(capsys):
    dcid = 5590481
    mimas.run([f"{dcid}", "-c"])
    captured = capsys.readouterr()
    expected = f"""\
At the start of data collection {dcid}:
 - do nothing

At the end of data collection {dcid}:
 - ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-autoPROC-eiger   
 - ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-fast-dp-eiger   --trigger 
 - ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-3dii-eiger   
 - ispyb.job --new --dcid={dcid} --source=automatic --recipe=autoprocessing-xia2-dials-eiger  --add-param=remove_blanks:true --trigger 
 - zocalo.go -r archive-nexus {dcid}
 - zocalo.go -r generate-crystal-thumbnails {dcid}
 - zocalo.go -r generate-diffraction-preview {dcid}
 - zocalo.go -r vmxi-per-image-analysis {dcid}"""
    assert expected in captured.out
