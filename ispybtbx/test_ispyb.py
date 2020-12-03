from datetime import datetime

from unittest import mock
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
    "borken_dcid": 2091234,
}


def test_ispyb_recipe_filtering_does_not_affect_messages_without_ispyb_content():
    message = {"dummy_msg": mock.sentinel.dummy_msg}
    parameters = {"dummy_param": mock.sentinel.dummy_param}

    message, parameters = ispyb_filter(message, parameters)

    assert message == {"dummy_msg": mock.sentinel.dummy_msg}
    assert parameters == {"dummy_param": mock.sentinel.dummy_param}


def test_ispyb_recipe_filtering_does_read_datacollection_information():
    message = {"dummy_msg": mock.sentinel.dummy_msg}
    parameters = {"dummy_param": mock.sentinel.dummy_param, "ispyb_dcid": ds["gphl_C2"]}

    message, parameters = ispyb_filter(message, parameters)

    assert message == {"dummy_msg": mock.sentinel.dummy_msg, "default_recipe": mock.ANY}
    for service in [
        "per-image-analysis-rotation",
        "processing-fast-dp",
        "processing-xia2-3dii",
        "processing-xia2-dials",
        "processing-autoproc",
    ]:
        assert service in message["default_recipe"]
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
        "/dls/i03/data/2016/cm14451-4/processed/tmp/2016-10-07/fake113556/TRP_M1S6_4_/"
    )
    assert parameters["ispyb_working_directory"].startswith(
        "/dls/i03/data/2016/cm14451-4/tmp/zocalo/tmp/2016-10-07/fake113556/TRP_M1S6_4_"
    )

    non_ispyb_parameters = {
        k: v for k, v in parameters.items() if not k.startswith("ispyb_")
    }
    assert non_ispyb_parameters == {"dummy_param": mock.sentinel.dummy_param}


def test_ispyb_recipe_filtering_is_successful_for_all_listed_examples():
    for example, dcid in ds.items():
        message = {}
        parameters = {"ispyb_dcid": dcid}
        message, parameters = ispyb_filter(message, parameters)
        assert message == {"default_recipe": mock.ANY}
        assert len(parameters) > 10


def test_fetch_datacollect_group_from_ispyb():
    i = ispybtbx()
    dc_id = ds["gphl_C2"]
    dc_info = i.get_dc_info(dc_id)
    assert dc_info
    assert dc_info["dataCollectionGroupId"]
    whole_group = i.get_related_dcs(dc_info["dataCollectionGroupId"])
    assert len(whole_group) == 1


def test_get_datacollection_information():
    i = ispybtbx()
    dc_id = ds["gphl_C2"]
    dc_info = i.get_dc_info(dc_id)
    # for k, v in dc_info.items():
    #   print(k, v)
    assert dc_info["fileTemplate"] == "TRP_M1S6_4_####.cbf"
    assert (
        dc_info["imageDirectory"]
        == "/dls/i03/data/2016/cm14451-4/tmp/2016-10-07/fake113556/"
    )
    assert dc_info["startTime"] == datetime(2016, 10, 7, 11, 47, 13)
    assert dc_info["endTime"] == datetime(2016, 10, 7, 11, 50, 56)
    assert dc_info["startImageNumber"] == 1
    assert dc_info["numberOfImages"] == 1800
    assert dc_info["overlap"] == 0.0
    assert dc_info["axisRange"] == 0.1
    assert dc_info["dataCollectionId"] == dc_id
    assert dc_info["imagePrefix"] == "TRP_M1S6"
    assert dc_info["wavelength"] == 0.979493
    assert dc_info["resolution"] == 1.5


def test_get_datacollection_information_for_em():
    i = ispybtbx()
    dc_id = ds["cryo_em"]
    dc_info = i.get_dc_info(dc_id)
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
    assert dc_info["startTime"] == datetime(2017, 11, 10, 14, 27, 7)
    assert dc_info["endTime"] == datetime(2017, 11, 14, 11, 28, 20)
    assert dc_info["startImageNumber"] is None  # because EM
    assert dc_info["numberOfImages"] is None  # because EM
    assert dc_info["overlap"] is None  # because EM
    assert dc_info["axisRange"] is None  # because EM
    assert dc_info["dataCollectionId"] == dc_id
    assert dc_info["imagePrefix"] is None  # because EM
    assert dc_info["wavelength"] == 0.0196875
    assert dc_info["resolution"] is None  # because EM


def test_datacollection_classification():
    i = ispybtbx()
    dc = {"axisRange": 0, "numberOfImages": 1800, "overlap": 0}
    assert i.classify_dc(dc) == {"grid": True, "rotation": False, "screen": False}

    dc = {"axisRange": 90, "numberOfImages": 1800, "overlap": 0}
    assert i.classify_dc(dc) == {"grid": False, "rotation": True, "screen": False}

    dc = {"axisRange": 90, "numberOfImages": 3, "overlap": -44.5}
    assert i.classify_dc(dc) == {"grid": False, "rotation": False, "screen": True}


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


def test_obtain_space_group():
    i = ispybtbx()
    dc_id = ds["sg_set"]
    sg, cell = i.get_space_group_and_unit_cell(dc_id)
    assert sg == "P212121"
    assert cell == (68.0, 84.0, 89.0, 90.0, 90.0, 90.0)


def test_obtain_sequence():
    i = ispybtbx()
    dc_id = ds["seq_set"]
    seq = i.get_protein_from_dcid(dc_id)["sequence"]
    assert (
        seq
        == "GPDKPVIKMYQIGDKPDNLDELLANANKIIEEKVGAKLDIQYLGWGDYGKKMSVITSSGENYDIAFADNYIVNAQKGAYADLTELYKKEGKDLYKALDPAYIK"
        "GNTVNGKIYAVPVAANVASSQNFAFNGTLLAKYGIDISGVTSYETLEPVLKQIKEKAPDVVPFAIGKVFIPSDNFDYPVANGLPFVIDLEGDTTKVVNRYEVPRFKEHLKTLHKFYE"
        "AGYIPKDVATSDTSFDLQQDTWFVREETVGPADYGNSLLSRVANKDIQIKPITNFIKKNQTTQVANFVISNNSKNKEKSMEILNLLNTNPELLNGLVYGPEGKNWEKIEGKENRVRV"
        "LDGYKGNTHMGGWNTGNNWILYINENVTDQQIENSKKELAEAKESPALGFIFNTDNVKSEISAIANTMQQFDTAINTGTVDPDKAIPELMEKLKSEGAYEKVLNEMQKQYDEFLKNKK\n"
    )


def test_obtain_edge_data():
    i = ispybtbx()
    dc_id = ds["edge_set"]
    param = i.get_energy_scan_from_dcid(dc_id)
    assert param["energyscanid"] == 52476
    assert param["atom_type"] == "Se"
    assert param["edge_position"] == "peak"


def test_filter_function():
    msg = {}
    param = {"ispyb_dcid": ds["i19_screening"]}
    msg, param = ispyb_filter(msg, param)


def test_retrieve_reprocessing_information():
    msg = {}
    param = {"ispyb_process": 95}
    msg, param = ispyb_filter(msg, param)
    assert msg == {"recipes": ["ispyb-xia2-dials"]}
    assert param["ispyb_process"] == 95
    assert param["ispyb_processing_job"]
    assert param["ispyb_processing_job"].comment == "Test entry by Markus"
    assert param["ispyb_processing_job"].DCID == 1956161
    assert param["ispyb_processing_job"].name == "Xia2 DIALS"
    assert param["ispyb_processing_job"].recipe == "xia2-dials"
    assert param["ispyb_processing_job"].timestamp
    assert param["ispyb_processing_job"].jobid == 95
    assert dict(param["ispyb_processing_job"].parameters) == {"d_min": "1.7"}
    assert len(param["ispyb_processing_job"].sweeps) == 1
    sweep = param["ispyb_processing_job"].sweeps[0]
    assert sweep.DCID == 1956161
    assert sweep.start == 20
    assert sweep.end == 580

    ## legacy:
    assert param["ispyb_reprocessing_parameters"] == {"d_min": "1.7"}


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


def test_get_sample_group_dcids_from_yml(tmpdir):
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
        "ispyb_dcid": 123456,
        "ispyb_dcid": 5660693,
        "ispyb_visit_directory": tmpdir,
        "ispyb_visit": "mx19946-377",
        "ispyb_image_directory": tmpdir / "VMXi-XY1234" / "well_144" / "images",
        "ispyb_image_template": "image_50934_master.h5",
    }
    groups = i.get_sample_group_dcids(ispyb_info)
    assert groups == [
        {"dcids": [5661104, 5661122, 5661125, 5661128, 5661131, 5661134, 5661137]},
        {"dcids": [5661122, 5661125, 5661128, 5661131, 5661134, 5661137]},
    ]
