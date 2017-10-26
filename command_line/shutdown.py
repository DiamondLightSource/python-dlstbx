#
# dlstbx.shutdown
#   Stop a dlstbx service
#

from __future__ import absolute_import, division

import json
import sys
from optparse import SUPPRESS_HELP, OptionParser

import workflows
from workflows.transport.stomp_transport import StompTransport

# Example: dlstbx.shutdown computer.12345
#          dlstbx.shutdown --all

if __name__ == '__main__':
  parser = OptionParser()

  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
# parser.add_option("--all", dest="all",
#     action="store_true", default=False,
#     help="Stop all dlstbx services (use with caution)")

  parser.add_option("--test", action="store_true", dest="test",
                    help="Run in ActiveMQ testing namespace (zocdev, default)")
  parser.add_option("--live", action="store_true", dest="test",
                    help="Run in ActiveMQ live namespace (zocalo)")

  # change settings when in live mode
  default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg'
  if '--live' in sys.argv:
    default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'

  StompTransport.load_configuration_file(default_configuration)
  StompTransport.add_command_line_options(parser)
  (options, args) = parser.parse_args(sys.argv[1:])
  stomp = StompTransport()

  if not len(args):
    print "Need to specify one or more services to shut down."
    print "Format: hostname.pid"
    sys.exit(1)

  stomp.connect()

  for host in args:
    if not host.startswith('uk.ac.diamond.'):
      host = 'uk.ac.diamond.' + host

    message = { 'command' : 'shutdown',
                'host': host,
              }

    stomp.broadcast(
      'command',
      message
    )
    print "Shutting down", host
