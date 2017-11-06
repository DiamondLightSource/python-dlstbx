from __future__ import division, print_function

from dlstbx.util.dxtbx_to_dozor import (dxtbx_to_dozor, write_dozor_input,
                                        parse_dozor_output)
from dxtbx import load

def main(args):
  '''Args take the form of a list of /path_to_dir/template_####.cbf:1:90
  arguments.'''

  for arg in args:
    pass


if __name__ == '__main__':
  import sys
  main(sys.argv[1:])
