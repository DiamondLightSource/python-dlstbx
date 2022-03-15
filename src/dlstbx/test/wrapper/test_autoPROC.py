from __future__ import annotations

import os
import pathlib

import pytest

from dlstbx.wrapper import autoPROC


@pytest.fixture
def autoproc_home(monkeypatch):
    """Set the $autoPROC_home variable

    Used for looking up beamline-specific macros.
    """
    monkeypatch.setenv("autoPROC_home", "/dls_sw/apps/GPhL/autoPROC/20211020")


@pytest.fixture
def durin_plugin_so(monkeypatch):
    """Add directory containing durin-plugin.so to the PATH"""
    path = pathlib.Path("/dls_sw/apps/XDS/20220110")
    monkeypatch.setenv("PATH", os.fspath(path), prepend=os.pathsep)
    return path / "durin-plugin.so"


@pytest.mark.skipif(
    not os.access(
        "/dls/i24/data/2022/cm31109-1/cut/TestThaumatin/thaumatin_11/", os.R_OK
    ),
    reason="Test images not available",
)
def test_construct_commandline_i24_cbf(autoproc_home):
    params = {
        "autoproc": {
            "pname": "cm31109v1",
            "xname": "xthaumatin111",
            "nproc": 12,
        },
        "images": "/dls/i24/data/2022/cm31109-1/cut/TestThaumatin/thaumatin_11/thaumatin_11_1_00001.cbf:1:1200",
        "beamline": "i24",
        "working_directory": "/dls/i24/data/2022/cm31109-1/tmp/zocalo/cut/TestThaumatin/thaumatin_11/thaumatin_11_1/3477adb0-00a2-4014-b2a7-a7fa68706c9c/autoPROC",
    }
    cmdline = autoPROC.construct_commandline(params)
    assert cmdline == [
        "process",
        "-xml",
        "autoPROC_XdsKeyword_MAXIMUM_NUMBER_OF_PROCESSORS=12",
        "-M",
        "HighResCutOnCChalf",
        'autoPROC_CreateSummaryImageHrefLink="no"',
        'autoPROC_Summary2Base64_Run="yes"',
        'StopIfSubdirExists="no"',
        "-d",
        "/dls/i24/data/2022/cm31109-1/tmp/zocalo/cut/TestThaumatin/thaumatin_11/thaumatin_11_1/3477adb0-00a2-4014-b2a7-a7fa68706c9c/autoPROC",
        "-nthreads",
        "12",
        "pname=cm31109v1",
        "xname=xthaumatin111",
        "-Id",
        "x0thaumatin111,/dls/i24/data/2022/cm31109-1/cut/TestThaumatin/thaumatin_11,thaumatin_11_1_#####.cbf,1,1200",
        'autoPROC_XdsKeyword_ROTATION_AXIS="0.000000 -1.000000  0.000000"',
        'autoPROC_XdsKeyword_UNTRUSTED_RECTANGLE="487 495 0 2528 | 981 989 0 2528 | '
        "1475 1483 0 2528 | 1969 1977 0 2528 | 0 2464 195 213 | 0 2464 407 425 | 0 "
        "2464 619 637 | 0 2464 831 849 | 0 2464 1043 1061 | 0 2464 1255 1273 | 0 2464 "
        "1467 1485 | 0 2464 1679 1697 | 0 2464 1891 1909 | 0 2464 2103 2121 | 0 2464 "
        '2315 2333"',
    ]


@pytest.mark.skipif(
    not os.access("/dls/i03/data/2022/cm31105-1/TestProteinaseK/protk_1", os.R_OK),
    reason="Test images not available",
)
def test_construct_commandline_i03_eiger(monkeypatch, autoproc_home, durin_plugin_so):
    nslots = "8"
    monkeypatch.setenv("NSLOTS", nslots)
    params = {
        "autoproc": {"pname": "cm31105v1", "xname": "xprotk117"},
        "images": "/dls/i03/data/2022/cm31105-1/TestProteinaseK/protk_1/protk_1_17_master.h5:1:3600",
        "beamline": "i03",
        "working_directory": "/dls/i03/data/2022/cm31105-1/tmp/zocalo/TestProteinaseK/protk_1/protk_1_17/5db2c356-19b2-4c16-82a4-3cec4a10d2d7",
        "ispyb_parameters": {
            "d_min": 1.5,
            "spacegroup": "P43212",
            "unit_cell": "68,68,107,90,90,90",
            "small_molecule": True,
        },
    }
    cmdline = autoPROC.construct_commandline(params)
    assert cmdline == [
        "process",
        "-xml",
        "autoPROC_XdsKeyword_MAXIMUM_NUMBER_OF_PROCESSORS=12",
        "-M",
        "HighResCutOnCChalf",
        'autoPROC_CreateSummaryImageHrefLink="no"',
        'autoPROC_Summary2Base64_Run="yes"',
        'StopIfSubdirExists="no"',
        "-d",
        "/dls/i03/data/2022/cm31105-1/tmp/zocalo/TestProteinaseK/protk_1/protk_1_17/5db2c356-19b2-4c16-82a4-3cec4a10d2d7",
        "-nthreads",
        nslots,
        "pname=cm31105v1",
        "xname=xprotk117",
        "-Id",
        "x0protk117,/dls/i03/data/2022/cm31105-1/TestProteinaseK/protk_1,protk_1_17_master.h5,1,3600",
        "DistributeBackgroundImagesForHdf5=no",
        f"autoPROC_XdsKeyword_LIB={durin_plugin_so}",
        "-R",
        "1000",
        "1.5",
        "symm=P43212",
        'cell="68 68 107 90 90 90"',
        "-M",
        "SmallMolecules",
    ]


@pytest.mark.skipif(
    not os.access(
        "/dls/i23/data/2022/cm31108-1/TestFerritin/20220210/Ferritin_x2/data_10_00001.cbf",
        os.R_OK,
    ),
    reason="Test images not available",
)
def test_construct_commandline_i23_macro(autoproc_home):
    params = {
        "autoproc": {"pname": "cm31108v1", "xname": "xdata10", "nproc": 5},
        "images": "/dls/i23/data/2022/cm31108-1/TestFerritin/20220210/Ferritin_x2/data_10_00001.cbf:1:3600",
        "beamline": "i23",
        "working_directory": "/dls/i23/data/2022/cm31108-1/tmp/zocalo/TestFerritin/20220210/Ferritin_x2/data_10/1d053370-7634-4812-bfbe-70f8856dea09/autoPROC",
    }
    cmdline = autoPROC.construct_commandline(params)
    assert cmdline == [
        "process",
        "-xml",
        "autoPROC_XdsKeyword_MAXIMUM_NUMBER_OF_PROCESSORS=12",
        "-M",
        "HighResCutOnCChalf",
        'autoPROC_CreateSummaryImageHrefLink="no"',
        'autoPROC_Summary2Base64_Run="yes"',
        'StopIfSubdirExists="no"',
        "-d",
        "/dls/i23/data/2022/cm31108-1/tmp/zocalo/TestFerritin/20220210/Ferritin_x2/data_10/1d053370-7634-4812-bfbe-70f8856dea09/autoPROC",
        "-nthreads",
        "5",
        "pname=cm31108v1",
        "xname=xdata10",
        "-M",
        "DiamondI23",
        "-Id",
        "x0data10,/dls/i23/data/2022/cm31108-1/TestFerritin/20220210/Ferritin_x2,data_10_#####.cbf,1,3600",
        'autoPROC_XdsKeyword_UNTRUSTED_RECTANGLE="2092 2468 2536 2600 | 1482 1970 '
        '2120 2316"',
    ]
