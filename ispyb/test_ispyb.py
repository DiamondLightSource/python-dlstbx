from __future__ import absolute_import, division
from datetime import datetime
from dlstbx.ispyb.ispyb import ispyb

ds = {
  "gphl_C2": 1397955,
  "i04_BAG": 527189,
  "weak_ins_4": 1383040,
  "sg_set": 1308505,
}

def test_fetch_datacollect_group_from_ispyb():
  i = ispyb()
  dc_id = ds['gphl_C2']
  res = i.get_dc_info(dc_id)
  # this was not recorded as a data collection group
  whole_group = i.get_dc_group(dc_id)
  assert len(whole_group) == 1
  # however there are four data collections
  whole_group = i.get_matching_folder(dc_id)
  assert len(whole_group) == 4
  for dc_id in whole_group:
    dc_info = i.get_dc_info(dc_id)

def test_get_datacollection_information():
  i = ispyb()
  dc_id = ds['gphl_C2']
  dc_info = i.get_dc_info(dc_id)
# for k, v in dc_info.iteritems():
#   print k, v
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

def test_datacollection_classification():
  i = ispyb()
  dc = { 'axisRange': 0, 'numberOfImages': 1800, 'overlap': 0 }
  assert i.classify_dc(dc) == { 'grid': True, 'rotation': False, 'screen': False }

  dc = { 'axisRange': 90, 'numberOfImages': 1800, 'overlap': 0 }
  assert i.classify_dc(dc) == { 'grid': False, 'rotation': True, 'screen': False }

  dc = { 'axisRange': 90, 'numberOfImages': 3, 'overlap': -44.5 }
  assert i.classify_dc(dc) == { 'grid': False, 'rotation': False, 'screen': True }

def test_get_first_file_of_datacollection():
  i = ispyb()
  dc = { 'imageDirectory': 'dir', 'fileTemplate': 'file_#####.cbf', 'startImageNumber': 30, 'numberOfImages': 300 }
  assert i.dc_info_to_filename(dc) == 'dir/file_00030.cbf'

def test_get_extent_of_filenames_for_datacollection():
  i = ispyb()
  dc = { 'imageDirectory': 'dir', 'fileTemplate': 'file_#####.cbf', 'startImageNumber': 30, 'numberOfImages': 300 }
  assert i.dc_info_to_start_end(dc) == (30, 329)

def test_obtain_space_group():
  i = ispyb()
  dc_id = ds['sg_set']
  sg = i.get_space_group(dc_id)
  assert sg == 'P212121'
