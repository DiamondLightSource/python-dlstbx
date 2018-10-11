#
# dlstbx.dc_sim_verify
#   Verify all outstanding simulated data collections against expected
#   results. Create a report as junit.xml file.
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

idlequeue = Queue.Queue()
def wait_until_idle(timelimit):
  try:
    while True:
      idlequeue.get(True, timelimit)
  except Queue.Empty:
    return

def process_result(header, message):
  idlequeue.put_nowait('start')

  print(message)  
  ##############################
  #
  #      Work happens here
  #
  ##############################

  idlequeue.put_nowait('done')

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.dc_sim_verify [options]")

  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("--test", action="store_true", dest="test", help="Run in ActiveMQ testing (zocdev) namespace")
  default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'
  dlqprefix = 'zocalo'
  if '--test' in sys.argv:
    default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg'
    dlqprefix = 'zocdev'
  # override default stomp host
  StompTransport.load_configuration_file(default_configuration)

  StompTransport.add_command_line_options(parser)
  (options, args) = parser.parse_args(sys.argv[1:])
  stomp = StompTransport()
  stomp.connect()

  txn = stomp.transaction_begin()
  sid = stomp.subscribe("transient.destination", process_result, acknowledgement=True, exclusive=True)
  wait_until_idle(3)
  stomp.unsubscribe(sid)
  wait_until_idle(1)
  stomp.transaction_commit(txn)


