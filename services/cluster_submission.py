from __future__ import absolute_import, division
from workflows.services.common_service import CommonService
from dials.util.procrunner import run_process
import os
import random
import string

class DLSClusterSubmission(CommonService):
  '''A service to run xia2 processing.'''

  # Human readable service name
  _service_name = "DLS cluster submitter"

  def initializing(self):
    '''Subscribe to the cluster submission queue.
       Received messages must be acknowledged.'''
    self._transport.subscribe('cluster_submit',
      self.run_submit_job,
      acknowledgement=True)

  def run_submit_job(self, header, message):
    '''Submit cluster job according to message.'''

    # Conditionally acknowledge receipt of the message
    txn = self._transport.transaction_begin()
    self._transport.ack(header['message-id'], transaction=txn)

    print header
    print message

    # Send results onwards
    new_header = {}
    if 'recipe' in header:
      new_header['recipe'] = header['recipe']
    self._transport.send('transient.destination', results, transaction=txn, header=new_header)
    self._transport.transaction_commit(txn)
