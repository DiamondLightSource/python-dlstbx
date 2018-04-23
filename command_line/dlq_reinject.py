#
# dlstbx.dlq_reinject
#   Take a dead letter queue message from a file and send it back to its queue
#   for a retry.
#

from __future__ import absolute_import, division, print_function

import errno
import json
import os
import Queue
import re
import sys
import time
from optparse import SUPPRESS_HELP, OptionParser

import workflows
from workflows.transport.stomp_transport import StompTransport

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.dlq_reinject [options] file [file [..]]")

  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("--test", action="store_true", dest="test", help="Run in ActiveMQ testing (zocdev) namespace")
  default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'
  redirect_live_to_testing = '--test' in sys.argv
  if redirect_live_to_testing:
    default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg'
  # override default stomp host
  StompTransport.load_configuration_file(default_configuration)

  StompTransport.add_command_line_options(parser)
  (options, args) = parser.parse_args()
  if not args:
    print("No DLQ message files given.")
    sys.exit(0)

  stomp = StompTransport()
  stomp.connect()
  dlqprefix = stomp.get_namespace()
  from pprint import pprint

  for dlqfile in args:
    with open(dlqfile, 'r') as fh:
      dlqmsg = json.load(fh)
    print("Parsing message from {}".format(dlqfile))
    if not isinstance(dlqmsg, dict) or not dlqmsg.get('header') or not dlqmsg.get('message'):
      sys.exit("File is not a valid DLQ message.")
    pprint(dlqmsg)

    def psend(*args, **kwargs):
      pprint(args)
      pprint(kwargs)

    destination = dlqmsg['header']['original-destination'].split('/', 2)
    if destination[1] == 'queue':
      print("sending...")
      send_function = stomp.send
    elif destination[1] == 'topic':
      print("broadcasting...")
      send_function = stomp.broadcast
    else:
      sys.exit("Cannot process message, unknown message mechanism")
    if redirect_live_to_testing and destination[2].startswith('zocalo.'):
      destination[2] = destination[2].replace('zocalo.', 'zocdev.', 1)
    header = dlqmsg['header']
    for drop_field in ('content-length', 'destination', 'expires', 'message-id', 'original-destination', 'originalExpiration', 'subscription', 'timestamp', 'redelivered'):
      if drop_field in header:
        del header[drop_field]
    send_function(destination[2], dlqmsg['message'], headers=header, ignore_namespace=True)
    print("Done.\n")

  stomp.disconnect()
