from __future__ import absolute_import, division, print_function

from datetime import datetime

import mock
from dlstbx.ispybtbx import ispyb_filter, ispybtbx

ds = {
  "gphl_C2": 1397955,
  "i04_BAG": 527189,
  "weak_ins_4": 1383040,
  "sg_set": 1308505,
  "i19_screening": 1396413,
  "cryo_em" : 2097825,
  "borken_dcid": 2091234,
}

def test_ispyb_recipe_filtering_does_not_affect_messages_without_ispyb_content():
  message = { 'dummy_msg': mock.sentinel.dummy_msg }
  parameters = { 'dummy_param': mock.sentinel.dummy_param }

  message, parameters = ispyb_filter(message, parameters)

  assert message == { 'dummy_msg': mock.sentinel.dummy_msg }
  assert parameters == { 'dummy_param': mock.sentinel.dummy_param }

def test_ispyb_recipe_filtering_does_read_datacollection_information():
  message = { 'dummy_msg': mock.sentinel.dummy_msg }
  parameters = { 'dummy_param': mock.sentinel.dummy_param, 'ispyb_dcid': ds['gphl_C2'] }

  message, parameters = ispyb_filter(message, parameters)

  assert message == { 'dummy_msg': mock.sentinel.dummy_msg, 'default_recipe': mock.ANY }
  for service in ['per-image-analysis-rotation', 'processing-fast-dp', 'processing-xia2-3dii', 'processing-xia2-dials', 'processing-multi-xia2-dials', 'processing-multi-xia2-3dii', 'processing-autoproc']:
    assert service in message['default_recipe']
  assert parameters['ispyb_beamline'] == 'i03'
  assert parameters['ispyb_dcid'] == ds['gphl_C2']
  assert isinstance(parameters['ispyb_dc_class'], dict)
  assert isinstance(parameters['ispyb_dc_info'], dict)
  assert parameters['ispyb_dc_class']['grid'] == False
  assert parameters['ispyb_image_first'] == 1
  assert parameters['ispyb_image_last'] == 1800
  assert parameters['ispyb_dc_info']['numberOfImages'] == 1800
  assert parameters['ispyb_image'] == '/dls/i03/data/2016/cm14451-4/tmp/2016-10-07/fake113556/TRP_M1S6_4_0001.cbf:1:1800'
  assert parameters['ispyb_image_pattern'] == 'TRP_M1S6_4_%04d.cbf'
  assert parameters['ispyb_images'] == '/dls/i03/data/2016/cm14451-4/tmp/2016-10-07/fake113556/TRP_M1S6_1_0001.cbf:1:1800,/dls/i03/data/2016/cm14451-4/tmp/2016-10-07/fake113556/TRP_M1S6_2_0001.cbf:1:1800,/dls/i03/data/2016/cm14451-4/tmp/2016-10-07/fake113556/TRP_M1S6_3_0001.cbf:1:1800'
  assert parameters['ispyb_results_directory'].startswith('/dls/i03/data/2016/cm14451-4/processed/tmp/2016-10-07/fake113556/TRP_M1S6_4_/')
  assert parameters['ispyb_working_directory'].startswith('/dls/i03/data/2016/cm14451-4/tmp/zocalo/tmp/2016-10-07/fake113556/TRP_M1S6_4_')

  non_ispyb_parameters = { k:v for k,v in parameters.iteritems() if not k.startswith('ispyb_') }
  assert non_ispyb_parameters == {
    'dummy_param': mock.sentinel.dummy_param
  }

def test_ispyb_recipe_filtering_is_successful_for_all_listed_examples():
  for example, dcid in ds.iteritems():
    message = {}
    parameters = { 'ispyb_dcid': dcid }
    message, parameters = ispyb_filter(message, parameters)
    assert message == { 'default_recipe': mock.ANY }
    assert len(parameters) > 10

def test_fetch_datacollect_group_from_ispyb():
  i = ispybtbx()
  dc_id = ds['gphl_C2']
  res = i.get_dc_info(dc_id)
  # this was not recorded as a data collection group
  whole_group = i.get_dc_group(dc_id)
  assert len(whole_group) == 1
  # however there are four data collections
  whole_group = i.get_matching_dcids_by_folder(dc_id)
  assert len(whole_group) == 4
  for dc_id in whole_group:
    dc_info = i.get_dc_info(dc_id)

def test_get_datacollection_information():
  i = ispybtbx()
  dc_id = ds['gphl_C2']
  dc_info = i.get_dc_info(dc_id)
# for k, v in dc_info.iteritems():
#   print(k, v)
  assert dc_info['fileTemplate'] == "TRP_M1S6_4_####.cbf"
  assert dc_info['imageDirectory'] == "/dls/i03/data/2016/cm14451-4/tmp/2016-10-07/fake113556/"
  assert dc_info['startTime'] == datetime(2016, 10, 7, 11, 47, 13)
  assert dc_info['endTime'] == datetime(2016, 10, 7, 11, 50, 56)
  assert dc_info['startImageNumber'] == 1
  assert dc_info['numberOfImages'] == 1800
  assert dc_info['overlap'] == 0.0
  assert dc_info['axisRange'] == 0.1
  assert dc_info['dataCollectionId'] == dc_id
  assert dc_info['imagePrefix'] == "TRP_M1S6"
  assert dc_info['wavelength'] == 0.979493
  assert dc_info['resolution'] == 1.5

def test_get_datacollection_information_for_em():
  i = ispybtbx()
  dc_id = ds['cryo_em']
  dc_info = i.get_dc_info(dc_id)
# for k, v in dc_info.iteritems():
#   print(k, v)
  assert dc_info['fileTemplate'] == "FoilHole_16386978_Data_16392706_16392707_20171017_0929-55644#####.mrc"
  assert dc_info['imageDirectory'] == "/dls/m02/data/2017/cm16766-5/processed/cm16766-5_20171110_1424/Runs/000002_ProtImportMovies/extra"
  assert dc_info['startTime'] == datetime(2017, 11, 10, 14, 27, 7)
  assert dc_info['endTime'] == datetime(2017, 11, 14, 11, 28, 20)
  assert dc_info['startImageNumber'] == None # because EM
  assert dc_info['numberOfImages'] == None # because EM
  assert dc_info['overlap'] == None # because EM
  assert dc_info['axisRange'] == None # because EM
  assert dc_info['dataCollectionId'] == dc_id
  assert dc_info['imagePrefix'] == None # because EM
  assert dc_info['wavelength'] == 0.0196875
  assert dc_info['resolution'] == None # because EM

def test_datacollection_classification():
  i = ispybtbx()
  dc = { 'axisRange': 0, 'numberOfImages': 1800, 'overlap': 0 }
  assert i.classify_dc(dc) == { 'grid': True, 'rotation': False, 'screen': False }

  dc = { 'axisRange': 90, 'numberOfImages': 1800, 'overlap': 0 }
  assert i.classify_dc(dc) == { 'grid': False, 'rotation': True, 'screen': False }

  dc = { 'axisRange': 90, 'numberOfImages': 3, 'overlap': -44.5 }
  assert i.classify_dc(dc) == { 'grid': False, 'rotation': False, 'screen': True }

def test_get_first_file_of_datacollection():
  i = ispybtbx()
  dc = { 'imageDirectory': 'dir', 'fileTemplate': 'file_#####.cbf', 'startImageNumber': 30, 'numberOfImages': 300 }
  assert i.dc_info_to_filename(dc) == 'dir/file_00030.cbf'

def test_get_extent_of_filenames_for_datacollection():
  i = ispybtbx()
  dc = { 'imageDirectory': 'dir', 'fileTemplate': 'file_#####.cbf', 'startImageNumber': 30, 'numberOfImages': 300 }
  assert i.dc_info_to_start_end(dc) == (30, 329)

def test_obtain_space_group():
  i = ispybtbx()
  dc_id = ds['sg_set']
  sg = i.get_space_group(dc_id)
  assert sg == 'P212121'

def test_filter_function():
  msg = {}
  param = { 'ispyb_dcid': ds["i19_screening"] }
  msg, param = ispyb_filter(msg, param)

def test_retrieve_reprocessing_information():
  msg = {}
  param = { 'ispyb_process': 95 }
  msg, param = ispyb_filter(msg, param)
  assert msg == { 'recipes': ['ispyb-xia2-dials'] }
  assert param['ispyb_process'] == 95
  assert param['ispyb_reprocessing_data']
  assert param['ispyb_reprocessing_data']['comments']
  assert param['ispyb_reprocessing_data']['dataCollectionId'] == 1956161
  assert param['ispyb_reprocessing_data']['displayName']
  assert param['ispyb_reprocessing_data']['recipe'] == 'xia2-dials'
  assert param['ispyb_reprocessing_data']['recordTimestamp']
  assert param['ispyb_reprocessing_data']['reprocessingId'] == 95
