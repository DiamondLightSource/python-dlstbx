#
# dlstbx.service
#   Starts a workflow service
#

from __future__ import division
from dlstbx.util.version import dlstbx_version
import dlstbx.workflow.transport
import dlstbx.workflow.frontend
from optparse import OptionParser, SUPPRESS_HELP
import sys

def run(cmdline_args):
  parser = OptionParser(
    usage='dlstbx.service [options]',
    version=dlstbx_version()
  )
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-s", "--service", dest="service", metavar="SVC",
      default=None, help="Name of the service to start, default '%default'")
  parser.add_option("-t", "--transport", dest="transport", metavar="TRN",
      default="stomp", help="Transport mechanism, default '%default'")
  dlstbx.workflow.transport.add_command_line_options(parser)
  (options, args) = parser.parse_args(cmdline_args)

  dlstbx.workflow.frontend.Frontend(
      service=options.service,
      transport=options.transport,
    ).run()

if __name__ == '__main__':
  run(sys.argv[1:])

