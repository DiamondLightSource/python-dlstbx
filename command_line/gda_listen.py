#
# dlstbx.gda_listen
#   Listen to all status messages for GDA
#

from __future__ import absolute_import, division, print_function

import sys
import time
from optparse import SUPPRESS_HELP, OptionParser

import workflows
from workflows.transport.stomp_transport import StompTransport

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.gda_listen [options]")

  parser.add_option("-?", action="help", help=SUPPRESS_HELP)

  # override default stomp host
  StompTransport.load_configuration_file(
    '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg')

  StompTransport.add_command_line_options(parser)
  (options, args) = parser.parse_args(sys.argv[1:])

  stomp = StompTransport()
  stomp.connect()

  def consume_message(header, message):
    print("\nReceived on %s:" % header['destination'])
    print(message.get('summary'))
    if message.get('URL'):
      print(message['URL'])
    if message.get('text'):
      print(message['text'])

  stomp.subscribe_broadcast('transient.report.>', consume_message)

  try:
    while True:
      time.sleep(60)
  except KeyboardInterrupt:
    print("\nDone.")
