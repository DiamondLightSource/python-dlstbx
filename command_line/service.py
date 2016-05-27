#
# dlstbx.service
#   Starts a workflow service
#

from __future__ import division
import dlstbx.workflow.transport
import dlstbx.workflow.frontend
from optparse import OptionParser
import sys

def run(cmdline_args):
  parser = OptionParser(
    usage='dlstbx.service [options]'
  )
  dlstbx.workflow.transport.add_command_line_options(parser)
  (options, args) = parser.parse_args(cmdline_args)

  dlstbx.workflow.frontend.Frontend()

if __name__ == '__main__':
  run(sys.argv[1:])

