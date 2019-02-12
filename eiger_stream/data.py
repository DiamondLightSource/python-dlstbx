from __future__ import absolute_import, division, print_function

import py

files = [
#  'end_of_series',
#  'frame_appendix',
  'header1',
  'header2',
  'streamfile_1',
  'streamfile_2',
  'streamfile_3',
  'streamfile_4',
]

base_dir = py.path.local('/dls_sw/work/tools/RHEL6-x86_64/eiger-detector/testdata/16_large')

valid_things = {
  filename: base_dir.join(filename).read_binary()
  for filename in files
}

if __name__ == '__main__':
  from pprint import pprint
  pprint(valid_things)
