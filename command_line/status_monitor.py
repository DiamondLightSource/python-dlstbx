#
# dlstbx.status_monitor
#   Starts a status monitor (what do you expect?)
#

from __future__ import division
from dlstbx.util.version import dlstbx_version
import workflows.contrib.status_monitor
import workflows.transport
from optparse import OptionParser, SUPPRESS_HELP
import sys

def run(cmdline_args):
  # override default stomp host
  from workflows.transport.stomp_transport import StompTransport
  try:
    StompTransport.load_configuration_file(
        '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg')
  except workflows.WorkflowsError, e:
    print e # probably should use logging

  parser = OptionParser(
    usage='dlstbx.status_monitor [options]',
    version=dlstbx_version()
  )
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-n", action="store_true", dest="nofancy",
      default=False, help="Do not draw fancy borders")
  parser.add_option("-t", "--transport", dest="transport", metavar="TRN",
      default="stomp", help="Transport mechanism, default '%default'")
  workflows.transport.add_command_line_options(parser)
  (options, args) = parser.parse_args(cmdline_args)

  monitor = workflows.contrib.status_monitor.Monitor(
      transport=options.transport,
    )
  if options.nofancy:
    monitor.border_chars = monitor.border_chars_text
  monitor.run()

if __name__ == '__main__':
  run(sys.argv[1:])

