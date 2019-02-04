from __future__ import absolute_import, division, print_function

import logging

import workflows.recipe
from dials.command_line.find_spots_server import work
from workflows.services.common_service import CommonService

from confluent_kafka import Consumer, KafkaError
# [[unassign] assign]
# [seek]
# consume

class DLSStreamAnalysis(CommonService):
  '''A service that analyses individual images from a stream.'''

  # Human readable service name
  _service_name = "DLS Stream-Analysis"

  # Logger name
  _logger_name = 'dlstbx.services.stream_analysis'

  def initializing(self):
    '''Subscribe to the stream_analysis queue. Received messages must be acknowledged.'''
    logging.getLogger('dials').setLevel(logging.WARNING)
    workflows.recipe.wrap_subscribe(self._transport, 'stream_analysis',
        self.stream_analysis, acknowledgement=True, log_extender=self.extend_log)

  def stream_analysis(self, rw, header, message):
    '''Run PIA on one image.'''

    # Extract the filename
    filename = message['file']

    # Set up PIA parameters
    parameters = rw.recipe_step.get('parameters', None)
    if parameters:
      parameters = ['{k}={v}'.format(k=k, v=v) for k, v in parameters.iteritems()]
    else:
      parameters = ['d_max=40']

    # Do the per-image-analysis
    self.log.debug("Running PIA on %s with parameters %s", filename, parameters)
    try:
      results = work(filename, cl=parameters)
    except Exception as e:
      self.log.error("PIA failed with %r", e, exc_info=True)
      rw.transport.nack(header)
      return

    # Pass through all file* fields
    for key in filter(lambda x: x.startswith('file'), message):
      results[key] = message[key]

    # Conditionally acknowledge receipt of the message
    txn = rw.transport.transaction_begin()
    rw.transport.ack(header, transaction=txn)

    # Send results onwards
    rw.set_default_channel('result')
    rw.send_to('result', results, transaction=txn)
    rw.transport.transaction_commit(txn)
    self.log.info("PIA completed on %s, %d spots found", filename, results['n_spots_total'])
