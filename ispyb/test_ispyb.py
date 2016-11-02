from __future__ import absolute_import, division

from dlstbx.ispyb.ispyb import ispyb

def test_fetch_datacollect_group_from_ispyb():
  i = ispyb()
  dc_id = 1397955
  res = i.get_dc_info(dc_id)
  # this was not recorded as a data collection group
  whole_group = i.get_dc_group(dc_id)
  assert len(whole_group) == 1
  # however there are four data collections
  whole_group = i.get_matching_folder(dc_id)
  assert len(whole_group) == 4
