#
# dlstbx.service_monitor
#   Starts a service monitor (what do you expect?)
#

from __future__ import division
from dlstbx.util.version import dlstbx_version
import dlstbx.workflow.status_monitor
import dlstbx.workflow.transport
from optparse import OptionParser, SUPPRESS_HELP
import sys

def run(cmdline_args):
  parser = OptionParser(
    usage='dlstbx.service_monitor [options]',
    version=dlstbx_version()
  )
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-t", "--transport", dest="transport", metavar="TRN",
      default="stomp", help="Transport mechanism, default '%default'")
  dlstbx.workflow.transport.add_command_line_options(parser)
  (options, args) = parser.parse_args(cmdline_args)

  dlstbx.workflow.status_monitor.Terminal(
      transport=options.transport,
    ).run()

if __name__ == '__main__':
  run(sys.argv[1:])

