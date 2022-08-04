from __future__ import annotations

import concurrent.futures
import json
import time
from unittest import mock

import ispyb.sqlalchemy
import pytest
import sqlalchemy
from sqlalchemy.orm import sessionmaker

import dlstbx.ispybtbx
from dlstbx.ispybtbx import ispyb_filter, ispybtbx

ds = {
    "gphl_C2": 1397955,
    "i04_BAG": 527189,
    "weak_ins_4": 1383040,
    "sg_set": 1308505,
    "seq_set": 2207772,
    "edge_set": 1722897,
    "i19_screening": 1396413,
    "cryo_em": 2097825,
    "cryo_em_tiff": 6351623,
    "borken_dcid": 2091234,
}


@pytest.fixture(scope="session")
def db_session():
    """Yields a SQLAlchemy connection which is rollbacked after the test"""

    Session = sessionmaker(
        bind=sqlalchemy.create_engine(
            ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
        )
    )
    session_ = Session()
    yield session_
    session_.rollback()
    session_.close()


def test_ispyb_recipe_filtering_does_not_affect_messages_without_ispyb_content(
    db_session,
):
    message = {"dummy_msg": mock.sentinel.dummy_msg}
    parameters = {"dummy_param": mock.sentinel.dummy_param}

    message, parameters = ispyb_filter(message, parameters, db_session)

    assert message == {"dummy_msg": mock.sentinel.dummy_msg}
    assert parameters == {"dummy_param": mock.sentinel.dummy_param}


def test_ispyb_recipe_filtering_does_read_datacollection_information(db_session):
    message = {"dummy_msg": mock.sentinel.dummy_msg}
    parameters = {"dummy_param": mock.sentinel.dummy_param, "ispyb_dcid": ds["gphl_C2"]}

    message, parameters = ispyb_filter(message, parameters, db_session)

    assert message == {"dummy_msg": mock.sentinel.dummy_msg}
    assert parameters["ispyb_beamline"] == "i03"
    assert parameters["ispyb_dcid"] == ds["gphl_C2"]
    assert isinstance(parameters["ispyb_dc_class"], dict)
    assert isinstance(parameters["ispyb_dc_info"], dict)
    assert parameters["ispyb_dc_class"]["grid"] is False
    assert parameters["ispyb_image_first"] == 1
    assert parameters["ispyb_image_last"] == 1800
    assert parameters["ispyb_dc_info"]["numberOfImages"] == 1800
    assert (
        parameters["ispyb_image"]
        == "/dls/i03/data/2016/cm14451-4/tmp/2016-10-07/fake113556/TRP_M1S6_4_0001.cbf:1:1800"
    )
    assert parameters["ispyb_image_pattern"] == "TRP_M1S6_4_%04d.cbf"
    assert parameters["ispyb_images"] == ""
    assert parameters["ispyb_visit"] == "cm14451-4"
    assert parameters["ispyb_visit_directory"] == "/dls/i03/data/2016/cm14451-4"
    assert parameters["ispyb_results_directory"].startswith(
        "/dls/i03/data/2016/cm14451-4/processed/tmp/2016-10-07/fake113556/TRP_M1S6_4/"
    )
    assert parameters["ispyb_working_directory"].startswith(
        "/dls/i03/data/2016/cm14451-4/tmp/zocalo/tmp/2016-10-07/fake113556/TRP_M1S6_4/"
    )

    non_ispyb_parameters = {
        k: v for k, v in parameters.items() if not k.startswith("ispyb_")
    }
    assert non_ispyb_parameters == {"dummy_param": mock.sentinel.dummy_param}


def test_ispyb_recipe_filtering_is_successful_for_all_listed_examples(db_session):
    for example, dcid in ds.items():
        message = {}
        parameters = {"ispyb_dcid": dcid}
        print(f"{example}: {dcid}")
        message, parameters = ispyb_filter(message, parameters, db_session)
        assert message == {}
        assert len(parameters) > 10


def test_ispyb_filtering_for_processing_job(db_session):
    message = {}
    parameters = {"ispyb_process": 6406100}
    message, parameters = ispyb_filter(message, parameters, db_session)
    assert (
        parameters["ispyb_images"]
        == "/dls/i04/data/2021/cm28182-1/20210204/TestProteinaseK/protk11/protk11_1_master.h5:1:3600"
    )
    assert parameters["ispyb_processing_job"] == {
        "recordTimestamp": "2021-02-04T11:37:24",
        "displayName": "",
        "dataCollectionId": 5898098,
        "processingJobId": 6406100,
        "automatic": 1,
        "comments": "",
        "recipe": "autoprocessing-xia2-dials-eiger",
    }
    assert message["recipes"] == ["ispyb-autoprocessing-xia2-dials-eiger"]
    assert parameters["ispyb_processing_parameters"] == {
        "resolution.cc_half_significance_level": ["0.1"]
    }


def test_fetch_datacollect_group_from_ispyb(db_session):
    i = ispybtbx()
    dc_id = ds["gphl_C2"]
    dc_info = i.get_dc_info(dc_id, db_session)
    assert dc_info
    assert dc_info["dataCollectionGroupId"]
    whole_group = i.get_related_dcs(dc_info["dataCollectionGroupId"], db_session)
    assert len(whole_group) == 1


def test_get_datacollection_information(db_session):
    i = ispybtbx()
    dc_id = ds["gphl_C2"]
    dc_info = i.get_dc_info(dc_id, db_session)
    # for k, v in dc_info.items():
    #   print(k, v)
    assert dc_info["fileTemplate"] == "TRP_M1S6_4_####.cbf"
    assert (
        dc_info["imageDirectory"]
        == "/dls/i03/data/2016/cm14451-4/tmp/2016-10-07/fake113556/"
    )
    assert dc_info["startTime"] == "2016-10-07T11:47:13"
    assert dc_info["endTime"] == "2016-10-07T11:50:56"
    assert dc_info["startImageNumber"] == 1
    assert dc_info["numberOfImages"] == 1800
    assert dc_info["overlap"] == 0.0
    assert dc_info["axisRange"] == 0.1
    assert dc_info["dataCollectionId"] == dc_id
    assert dc_info["imagePrefix"] == "TRP_M1S6"
    assert dc_info["wavelength"] == 0.979493
    assert dc_info["resolution"] == 1.5


def test_get_datacollection_information_for_em(db_session):
    i = ispybtbx()
    dc_id = ds["cryo_em"]
    dc_info = i.get_dc_info(dc_id, db_session)
    # for k, v in dc_info.items():
    #   print(k, v)
    assert (
        dc_info["fileTemplate"]
        == "FoilHole_16386978_Data_16392706_16392707_20171017_0929-55644#####.mrc"
    )
    assert (
        dc_info["imageDirectory"]
        == "/dls/m02/data/2017/cm16766-5/processed/cm16766-5_20171110_1424/Runs/000002_ProtImportMovies/extra"
    )
    assert dc_info["startTime"] == "2017-11-10T14:27:07"
    assert dc_info["endTime"] == "2017-11-14T11:28:20"
    assert dc_info["startImageNumber"] is None  # because EM
    assert dc_info["numberOfImages"] is None  # because EM
    assert dc_info["overlap"] is None  # because EM
    assert dc_info["axisRange"] is None  # because EM
    assert dc_info["dataCollectionId"] == dc_id
    assert dc_info["imagePrefix"] is None  # because EM
    assert dc_info["wavelength"] == 0.0196875
    assert dc_info["resolution"] is None  # because EM


def test_get_datacollection_information_for_em_tiffs(db_session):
    i = ispybtbx()
    dc_id = ds["cryo_em_tiff"]
    dc_info = i.get_dc_info(dc_id, db_session)
    assert dc_info["fileTemplate"] == "Frames/*.tiff"
    assert dc_info["imageDirectory"] == "/dls/m02/data/2021/bi23047-54/raw/"
    assert dc_info["startTime"] == "2021-05-18T16:31:35"
    assert dc_info["endTime"] == "2021-05-18T16:31:35"
    assert dc_info["startImageNumber"] is None  # because EM
    assert dc_info["numberOfImages"] is None  # because EM
    assert dc_info["overlap"] is None  # because EM
    assert dc_info["axisRange"] is None  # because EM
    assert dc_info["dataCollectionId"] == dc_id
    assert dc_info["imagePrefix"] is None  # because EM
    assert dc_info["imageSuffix"] == "tiff"
    assert dc_info["wavelength"] is None  # not guaranteed to be set
    assert dc_info["resolution"] is None  # because EM

    message = {"dummy_msg": mock.sentinel.dummy_msg}
    parameters = {
        "dummy_param": mock.sentinel.dummy_param,
        "ispyb_dcid": ds["cryo_em_tiff"],
    }
    message, parameters = ispyb_filter(message, parameters, db_session)
    assert (
        parameters["ispyb_image_pattern"]
        == parameters["ispyb_image_template"]
        == "Frames/*.tiff"
    )
    results_directory = "/".join(parameters["ispyb_results_directory"].split("/")[:-1])
    working_directory = "/".join(parameters["ispyb_working_directory"].split("/")[:-1])
    # waiting for Synchweb to generate DCs with dataCollectionNumber set to NULL
    assert results_directory  # == '/dls/m02/data/2021/bi23047-54/processed/raw'
    assert working_directory  # == '/dls/m02/data/2021/bi23047-54/tmp/zocalo/raw'


def test_datacollection_classification():
    i = ispybtbx()
    dc = {
        "axisRange": 0,
        "numberOfImages": 1800,
        "overlap": 0,
        "gridinfo": {"steps_x": 1, "steps_y": 80},
    }
    assert i.classify_dc(dc, None) == {
        "grid": True,
        "rotation": False,
        "screen": False,
        "serial_fixed": False,
    }

    i = ispybtbx()
    dc = {"axisRange": 0, "numberOfImages": 1, "overlap": 0, "gridinfo": {}}
    assert i.classify_dc(dc, None) == {
        "grid": False,
        "rotation": False,
        "screen": True,
        "serial_fixed": False,
    }

    dc = {"axisRange": 90, "numberOfImages": 1800, "overlap": 0}
    assert i.classify_dc(dc, None) == {
        "grid": False,
        "rotation": True,
        "screen": False,
        "serial_fixed": False,
    }

    dc = {"axisRange": 90, "numberOfImages": 3, "overlap": -44.5}
    assert i.classify_dc(dc, None) == {
        "grid": False,
        "rotation": False,
        "screen": True,
        "serial_fixed": False,
    }

    dc = {"axisRange": 0, "numberOfImages": 25600, "overlap": 0}
    assert i.classify_dc(dc, "Serial Fixed") == {
        "grid": False,
        "rotation": False,
        "screen": False,
        "serial_fixed": True,
    }


def test_get_first_file_of_datacollection():
    i = ispybtbx()
    dc = {
        "imageDirectory": "dir",
        "fileTemplate": "file_#####.cbf",
        "startImageNumber": 30,
        "numberOfImages": 300,
    }
    assert i.dc_info_to_filename(dc) == "dir/file_00030.cbf"


def test_get_extent_of_filenames_for_datacollection():
    i = ispybtbx()
    dc = {
        "imageDirectory": "dir",
        "fileTemplate": "file_#####.cbf",
        "startImageNumber": 30,
        "numberOfImages": 300,
    }
    assert i.dc_info_to_start_end(dc) == (30, 329)


def test_obtain_space_group(db_session):
    i = ispybtbx()
    dc_id = ds["sg_set"]
    sg, cell = i.get_space_group_and_unit_cell(dc_id, db_session)
    assert sg == "P212121"
    assert cell == (68.0, 84.0, 89.0, 90.0, 90.0, 90.0)
    assert json.dumps(cell) == "[68.0, 84.0, 89.0, 90.0, 90.0, 90.0]"


def test_obtain_sequence(db_session):
    i = ispybtbx()
    dc_id = ds["seq_set"]
    seq = i.get_protein_from_dcid(dc_id, db_session)["sequence"]
    assert (
        seq
        == "GPDKPVIKMYQIGDKPDNLDELLANANKIIEEKVGAKLDIQYLGWGDYGKKMSVITSSGENYDIAFADNYIVNAQKGAYADLTELYKKEGKDLYKALDPAYIK"
        "GNTVNGKIYAVPVAANVASSQNFAFNGTLLAKYGIDISGVTSYETLEPVLKQIKEKAPDVVPFAIGKVFIPSDNFDYPVANGLPFVIDLEGDTTKVVNRYEVPRFKEHLKTLHKFYE"
        "AGYIPKDVATSDTSFDLQQDTWFVREETVGPADYGNSLLSRVANKDIQIKPITNFIKKNQTTQVANFVISNNSKNKEKSMEILNLLNTNPELLNGLVYGPEGKNWEKIEGKENRVRV"
        "LDGYKGNTHMGGWNTGNNWILYINENVTDQQIENSKKELAEAKESPALGFIFNTDNVKSEISAIANTMQQFDTAINTGTVDPDKAIPELMEKLKSEGAYEKVLNEMQKQYDEFLKNKK\n"
    )


def test_obtain_edge_data(db_session):
    i = ispybtbx()
    dc_id = ds["edge_set"]
    param = i.get_energy_scan_from_dcid(dc_id, db_session)
    assert param["energyscanid"] == 52476
    assert param["atom_type"] == "Se"
    assert param["edge_position"] == "peak"


def test_filter_function(db_session):
    msg = {}
    param = {"ispyb_dcid": ds["i19_screening"]}
    msg, param = ispyb_filter(msg, param, db_session)


def test_filter_function_with_load_config_file_timeout(monkeypatch, db_session):
    def mock_load_config_file(*args, **kwargs):
        time.sleep(2)

    msg = {}
    param = {"ispyb_dcid": ds["i19_screening"]}

    with monkeypatch.context() as m, pytest.raises(concurrent.futures.TimeoutError):
        m.setattr(
            dlstbx.ispybtbx, "load_sample_group_config_file", mock_load_config_file
        )
        msg, param = ispyb_filter(msg, param, db_session, io_timeout=1)

    with monkeypatch.context() as m, pytest.raises(concurrent.futures.TimeoutError):
        m.setattr(dlstbx.ispybtbx, "load_configuration_file", mock_load_config_file)
        msg, param = ispyb_filter(msg, param, db_session, io_timeout=1)


def test_load_sample_group_config_file(tmpdir):
    (tmpdir / "processing").mkdir()
    config_file = tmpdir / "processing" / "sample_groups.yml"
    config_file.write(
        """\
- [well_1, well_2, well_3]
- [well_121, well_122, well_123]
- [well_1, well_123]
"""
    )
    ispyb_info = {
        "ispyb_visit_directory": tmpdir,
        "ispyb_image_directory": tmpdir / "VMXi-XY1234" / "well_123" / "images",
        "ispyb_image_template": "image_50934_master.h5",
    }
    group = dlstbx.ispybtbx.load_sample_group_config_file(ispyb_info)
    assert group == [
        ["well_121", "well_122", "well_123"],
        ["well_1", "well_123"],
    ]


def test_get_sample_group_dcids_from_yml(tmpdir, db_session):
    (tmpdir / "processing").mkdir()
    config_file = tmpdir / "processing" / "sample_groups.yml"
    config_file.write(
        """\
- [well_143, well_144]
- [well_144, well_145]
"""
    )
    i = ispybtbx()
    ispyb_info = {
        "ispyb_dcid": 5660693,
        "ispyb_visit_directory": tmpdir,
        "ispyb_visit": "mx19946-377",
        "ispyb_image_directory": tmpdir / "VMXi-XY1234" / "well_144" / "images",
        "ispyb_image_template": "image_50934_master.h5",
        "ispyb_dc_info": {
            "SESSIONID": 27444332,
        },
    }
    groups = i.get_sample_group_dcids(ispyb_info, db_session)
    assert groups == [
        {"dcids": [5661104, 5661122, 5661125, 5661128, 5661131, 5661134, 5661137]},
        {"dcids": [5661122, 5661125, 5661128, 5661131, 5661134, 5661137]},
    ]


def test_get_related_dcids_same_directory(db_session):
    i = ispybtbx()
    assert i.get_related_dcids_same_directory(5646632, db_session) == {
        "dcids": [
            5646578,
            5646584,
            5646590,
            5646596,
            5646602,
            5646608,
            5646614,
            5646620,
            5646626,
        ]
    }


def test_get_sample_group_dcids(db_session):
    i = ispybtbx()
    related_dcids = i.get_sample_group_dcids(
        {
            "ispyb_dcid": 5469646,
            "ispyb_dc_info": {
                "SESSIONID": 27441067,
            },
        },
        db_session,
    )
    assert related_dcids == [
        {
            "dcids": [5469637, 5469640, 5469643, 5469646],
            "sample_group_id": 307,
            "name": None,
        },
        {
            "dcids": [5469637, 5469640, 5469643, 5469646],
            "sample_group_id": 310,
            "name": None,
        },
        {
            "dcids": [
                5336272,
                5469586,
                5469589,
                5469592,
                5469595,
                5469637,
                5469640,
                5469643,
                5469646,
            ],
            "sample_group_id": 313,
            "name": None,
        },
    ]


def test_get_related_dcs(db_session):
    assert ispybtbx().get_related_dcs(5339105, db_session) == [5898098, 5898104]


def test_get_dcid_for_path(db_session):
    assert (
        ispybtbx().get_dcid_for_path(
            "/dls/i04/data/2021/cm28182-1/20210305/TestThaumatin/Se-Thaumatin8/Se-Thaumatin8_1_master.h5",
            db_session,
        )
        == 6077651
    )


def test_get_diffractionplan_from_dcid(db_session):
    diffractionplan = ispybtbx().get_diffractionplan_from_dcid(5898098, db_session)
    assert {
        "diffractionPlanId",
        "experimentKind",
        "centringMethod",
        "preferredBeamSizeX",
        "preferredBeamSizeY",
        "exposureTime",
        "requiredResolution",
        "radiationSensitivity",
        "anomalousScatterer",
        "energy",
    } <= diffractionplan.keys()


def test_get_gridscan_info(db_session):
    assert ispybtbx().get_gridscan_info(6077465, db_session) == {
        "dataCollectionId": 6077465,
        "snaked": 1,
        "orientation": "horizontal",
        "recordTimeStamp": "2021-03-05T15:29:20",
        "pixelsPerMicronX": 0.566,
        "pixelsPerMicronY": 0.566,
        "steps_x": 27.0,
        "dx_mm": 0.02,
        "xOffset": None,
        "snapshot_offsetXPixel": 77.0,
        "snapshot_offsetYPixel": 50.8881,
        "steps_y": 10.0,
        "yOffset": None,
        "dy_mm": 0.02,
        "dataCollectionGroupId": 5492072,
        "meshAngle": None,
        "gridInfoId": 1307711,
        "workflowMeshId": None,
    }


def test_get_sample_dcids(db_session):
    assert ispybtbx().get_sample_dcids(3297161, db_session) == {
        "dcids": [
            5990969,
            5990975,
            5990978,
            6074243,
            6074252,
            6074267,
            6074273,
            6074339,
            6074354,
            6074489,
            6076046,
            6076058,
            6076334,
            6076376,
            6077465,
            6077489,
            6077651,
        ],
        "sample_id": 3297161,
        "name": "Se-Thaumatin8",
    }


def test_get_priority_processing_for_dc_info(db_session):
    assert (
        ispybtbx().get_priority_processing_for_dc_info(
            {"BLSAMPLEID": 3297161}, db_session
        )
        == "xia2/DIALS"
    )


def test_ready_for_processing(db_session):
    message = {}
    parameters = {"ispyb_wait_for_runstatus": True, "ispyb_dcid": 5990969}
    assert dlstbx.ispybtbx.ready_for_processing(message, parameters, db_session) is True
    parameters = {"ispyb_dcid": 5990969}
    assert dlstbx.ispybtbx.ready_for_processing(message, parameters, db_session) is True
    parameters = {"ispyb_wait_for_runstatus": False, "ispyb_dcid": 5990969}
    assert dlstbx.ispybtbx.ready_for_processing(message, parameters, db_session) is True


def test_get_dcg_dcids(db_session):
    assert ispybtbx().get_dcg_dcids(6222263, 5617586, db_session) == [6222221, 6222245]
    msg, param = ispyb_filter({}, {"ispyb_dcid": 6222263}, db_session)
    assert param["ispyb_dcg_dcids"] == [6222221, 6222245]


def test_dcg_experiment_type(db_session):
    _, params = ispyb_filter({}, {"ispyb_dcid": 6903084}, db_session)
    assert params["ispyb_dcg_experiment_type"] == "Mesh"
    _, params = ispyb_filter({}, {"ispyb_dcid": 6921153}, db_session)
    assert params["ispyb_dcg_experiment_type"] == "SAD"
