from __future__ import absolute_import, division
from workflows.services.common_service import CommonService
from dials.util.procrunner import run_process
import os
import random
import string

class DLSRunXia2(CommonService):
  '''A service to run xia2 processing.'''

  # Human readable service name
  _service_name = "DLS xia2"

  def initializing(self):
    '''Subscribe to the xia2 job queue. Received messages must be acknowledged.'''
    # TODO: Limit the number of messages in flight
    self._transport.subscribe('run_xia2', self.run_xia2_job, acknowledgement=True)

  def run_xia2_job(self, header, message):
    '''Run xia2 according to one message.'''

    # Conditionally acknowledge receipt of the message
    txn = self._transport.transaction_begin()
    self._transport.ack(header, transaction=txn)

    parameters = message.get('parameters', [])
    command_line = ['xia2'] + parameters
    working_directory = '/dls/tmp/' + ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(16))
    print "Working directory:", working_directory

    os.mkdir(working_directory)
    os.chdir(working_directory)

    # Run the process. 1 hour timeout
    results = run_process(command_line, timeout=3600)

    # Send results onwards
    self._transport.send('transient.destination', results, transaction=txn)
    self._transport.transaction_commit(txn)
    print "xia2 run completed"
