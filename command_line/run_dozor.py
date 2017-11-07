from __future__ import division, print_function
from dials.util import procrunner

from dlstbx.util.dxtbx_to_dozor import (dxtbx_to_dozor, write_dozor_input,
                                        parse_dozor_output)

from dxtbx.datablock import DataBlockTemplateImporter
from dxtbx.datablock import DataBlockFactory

def scanner(arg):

  if '?' in arg:
    arg = arg.replace('?', '#')

  if ':' in arg:
    tokens = arg.split(':')
    template = tokens[0]
    start = int(tokens[1])
    end = int(tokens[2])
  else:
    template = arg
    start = 0
    end = 0

  importer = DataBlockTemplateImporter([template], 0)
  datablocks = importer.datablocks
  assert len(datablocks) == 1
  datablock = datablocks[0]

  imagesets = datablock.extract_imagesets()
  assert len(imagesets) == 1

  imageset = imagesets[0]

  dozor = dxtbx_to_dozor(imageset)

  dozor['name_template_image'] = imageset.get_template().replace('#', '?')
  if (start > 0) and (end > 0):
    dozor['first_image_number'] = start
    dozor['number_images'] = end - start + 1

  write_dozor_input(dozor, 'dozor.in')

  command = ['dozor.rh6', 'dozor.in']

  result = procrunner.run_process(
    command, timeout=3600.0,
    print_stdout=False, print_stderr=False)

  # results are a dictionary keyed by image number, containing main score,
  # spot score and observed resolution
  results = parse_dozor_output(result['stdout'])

  for image in sorted(results):
    print(image, results[image])

  return results

def main(args):
  '''Args take the form of a list of /path_to_dir/template_####.cbf:1:90
  arguments.'''

  for arg in args:
    scanner(arg)

if __name__ == '__main__':
  import sys
  main(sys.argv[1:])
