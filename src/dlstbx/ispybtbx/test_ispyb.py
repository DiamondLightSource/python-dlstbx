from decimal import Decimal
from unittest import mock

from ispyb.sqlalchemy import DataCollection

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
        "/dls/i03/data/2016/cm14451-4/processed/tmp/2016-10-07/fake113556/TRP_M1S6_4/"
    )
    assert parameters["ispyb_working_directory"].startswith(
        "/dls/i03/data/2016/cm14451-4/tmp/zocalo/tmp/2016-10-07/fake113556/TRP_M1S6_4/"
    )

    non_ispyb_parameters = {
        k: v for k, v in parameters.items() if not k.startswith("ispyb_")
    }
    assert non_ispyb_parameters == {"dummy_param": mock.sentinel.dummy_param}


def test_ispyb_recipe_filtering_is_successful_for_all_listed_examples():
    for example, dcid in ds.items():
        message = {}
        parameters = {"ispyb_dcid": dcid}
        print(f"{example}: {dcid}")
        message, parameters = ispyb_filter(message, parameters)
        assert message == {"default_recipe": mock.ANY}
        assert len(parameters) > 10


def test_ispyb_filtering_for_processing_job():
    message = {}
    parameters = {"ispyb_process": 6406100}
    message, parameters = ispyb_filter(message, parameters)
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


def test_fetch_datacollect_group_from_ispyb():
    i = ispybtbx()
    dcid = ds["gphl_C2"]
    dc = i.get_data_collection(dcid)
    assert dc
    assert dc.dataCollectionGroupId
    whole_group = i.get_related_dcs(dc)
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


def test_data_collection_classification():
    i = ispybtbx()
    dc = DataCollection(axisRange=0, numberOfImages=1800, overlap=0)
    assert i.classify_data_collection(dc) == {
        "grid": True,
        "rotation": False,
        "screen": False,
    }

    dc = DataCollection(axisRange=90, numberOfImages=1800, overlap=0)
    assert i.classify_data_collection(dc) == {
        "grid": False,
        "rotation": True,
        "screen": False,
    }

    dc = DataCollection(axisRange=90, numberOfImages=3, overlap=-44.5)
    assert i.classify_data_collection(dc) == {
        "grid": False,
        "rotation": False,
        "screen": True,
    }


def test_get_first_file_of_datacollection():
    i = ispybtbx()
    dc = DataCollection(
        imageDirectory="dir",
        fileTemplate="file_#####.cbf",
        startImageNumber=30,
        numberOfImages=300,
    )
    assert i.get_filename(dc) == "dir/file_00030.cbf"


def test_get_extent_of_filenames_for_datacollection():
    i = ispybtbx()
    dc = DataCollection(
        imageDirectory="dir",
        fileTemplate="file_#####.cbf",
        startImageNumber=30,
        numberOfImages=300,
    )
    assert i.get_start_end(dc) == (30, 329)


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


def test_load_configuration_file(tmp_path):
    visit_dir = tmp_path / "mx12345-6"
    (visit_dir / "processing").mkdir(parents=True)
    config_file = visit_dir / "processing" / "Therm.yml"
    config_file.write_text(
        """\
ispyb_unit_cell: [57.7, 57.7, 149.8, 90, 90, 90]
ispyb_space_group:  P 41 21 2
"""
    )
    dc = DataCollection(
        imageDirectory=str(visit_dir / "Therm"),
        fileTemplate="image_1_#####.cbf",
    )
    assert dlstbx.ispybtbx.load_configuration_file(dc) == {
        "ispyb_unit_cell": [57.7, 57.7, 149.8, 90, 90, 90],
        "ispyb_space_group": "P 41 21 2",
    }


def test_load_sample_group_config_file(tmp_path):
    visit_dir = tmp_path / "mx12345-6"
    (visit_dir / "processing").mkdir(parents=True)
    config_file = visit_dir / "processing" / "sample_groups.yml"
    config_file.write_text(
        """\
- [well_1, well_2, well_3]
- [well_121, well_122, well_123]
- [well_1, well_123]
"""
    )
    dc = DataCollection(
        imageDirectory=str(visit_dir / "VMXi-XY1234" / "well_123" / "images"),
        fileTemplate="image_50934_master.h5",
    )
    group = dlstbx.ispybtbx.load_sample_group_config_file(dc)
    assert group == [
        ["well_121", "well_122", "well_123"],
        ["well_1", "well_123"],
    ]


def test_get_sample_group_dcids_from_yml(tmp_path):
    visit_dir = tmp_path / "mx12345-6"
    (visit_dir / "processing").mkdir(parents=True)
    config_file = visit_dir / "processing" / "sample_groups.yml"
    config_file.write_text(
        """\
- [well_143, well_144]
- [well_144, well_145]
"""
    )
    i = ispybtbx()
    dc = DataCollection(
        dataCollectionId=5660693,
        SESSIONID=27444332,
        imageDirectory=str(visit_dir / "VMXi-XY1234" / "well_144" / "images"),
        fileTemplate="image_50934_master.h5",
    )
    groups = i.get_sample_group_dcids(dc)
    assert groups == [
        {"dcids": [5661104, 5661122, 5661125, 5661128, 5661131, 5661134, 5661137]},
        {"dcids": [5661122, 5661125, 5661128, 5661131, 5661134, 5661137]},
    ]


def test_get_related_dcids_same_directory():
    i = ispybtbx()
    assert i.get_related_dcids_same_directory(5646632) == {
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


def test_get_sample_group_dcids():
    i = ispybtbx()
    related_dcids = i.get_sample_group_dcids(i.get_data_collection(5469646))
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


def test_get_related_dcs():
    i = ispybtbx()
    dc = i.get_data_collection(5898098)
    related_dcs = ispybtbx().get_related_dcs(dc)
    assert [d.dataCollectionId for d in related_dcs] == [5898098, 5898104]


def test_get_dcid_for_path():
    assert (
        ispybtbx().get_dcid_for_path(
            "/dls/i04/data/2021/cm28182-1/20210305/TestThaumatin/Se-Thaumatin8/Se-Thaumatin8_1_master.h5"
        )
        == 6077651
    )


def test_get_diffractionplan_from_dcid():
    diffractionplan = ispybtbx().get_diffractionplan_from_dcid(5898098)
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


def test_get_gridscan_info():
    dc = DataCollection(
        dataCollectionGroupId=5492072,
        dataCollectionId=6077465,
    )
    assert ispybtbx().get_gridscan_info(dc) == {
        "dataCollectionId": None,
        "snaked": 1,
        "orientation": "horizontal",
        "recordTimeStamp": "2021-03-05T15:29:20",
        "pixelsPerMicronX": 0.566,
        "pixelsPerMicronY": 0.566,
        "steps_x": Decimal("27.0000000000"),
        "dx_mm": Decimal("0.0200000000"),
        "xOffset": None,
        "snapshot_offsetXPixel": 77.0,
        "snapshot_offsetYPixel": 50.8881,
        "steps_y": Decimal("10.0000000000"),
        "yOffset": None,
        "dy_mm": Decimal("0.0200000000"),
        "dataCollectionGroupId": 5492072,
        "meshAngle": None,
        "gridInfoId": 1307711,
        "workflowMeshId": None,
    }


def test_get_sample_dcids():
    i = ispybtbx()
    dc = i.get_data_collection(6077651)
    assert i.get_sample_dcids(dc) == {
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


def test_get_priority_processing():
    dc = ispybtbx().get_data_collection(6077465)
    assert ispybtbx().get_priority_processing(dc) == "xia2/DIALS"

    dc = DataCollection()
    assert ispybtbx().get_priority_processing(dc) is None


def test_ready_for_processing():
    message = {}
    parameters = {"ispyb_wait_for_runstatus": True, "ispyb_dcid": 5990969}
    assert dlstbx.ispybtbx.ready_for_processing(message, parameters) is True
    parameters = {"ispyb_dcid": 5990969}
    assert dlstbx.ispybtbx.ready_for_processing(message, parameters) is True
    parameters = {"ispyb_wait_for_runstatus": False, "ispyb_dcid": 5990969}
    assert dlstbx.ispybtbx.ready_for_processing(message, parameters) is True


def test_get_detector_class():
    dc = ispybtbx().get_data_collection(5990969)
    assert ispybtbx().get_detector_class(dc) == "eiger"

    dc = ispybtbx().get_data_collection(5881028)
    assert ispybtbx().get_detector_class(dc) == "pilatus"


def test_get_filename_pattern():
    dc = DataCollection(fileTemplate="image_5_master.h5")
    assert ispybtbx().get_filename_pattern(dc) == "image_5_master.h5"
    dc = DataCollection(fileTemplate="image_1_#####.cbf")
    assert ispybtbx().get_filename_pattern(dc) == "image_1_%05d.cbf"
    dc = DataCollection(fileTemplate="image_%_#####.cbf")
    assert ispybtbx().get_filename_pattern(dc) == "image_%%_%05d.cbf"


def test_get_filename():
    dc = DataCollection(imageDirectory="/foo", fileTemplate="image_5_master.h5")
    assert ispybtbx().get_filename(dc) == "/foo/image_5_master.h5"
    dc = DataCollection(
        imageDirectory="/foo", startImageNumber=5, fileTemplate="image_1_#####.cbf"
    )
    assert ispybtbx().get_filename(dc) == "/foo/image_1_00005.cbf"
    dc = DataCollection(
        imageDirectory="/foo", startImageNumber=5, fileTemplate="image_1_#####.cbf"
    )
    assert ispybtbx().get_filename(dc, image_number=3) == "/foo/image_1_00003.cbf"
