from __future__ import absolute_import, division
from dials.command_line.find_spots_client import response_to_xml
from dials.command_line.find_spots_server import work
import logging
import os
import time
from workflows.services.common_service import CommonService

class DLSPerImageAnalysis(CommonService):
  '''A service that analyses individual images.'''

  # Human readable service name
  _service_name = "DLS Per-Image-Analysis"

  # Logger name
  _logger_name = 'dlstbx.services.per_image_analysis'

  def initializing(self):
    '''Subscribe to the per_image_analysis queue. Received messages must be acknowledged.'''
    # TODO: Limit the number of messages in flight
    self._transport.subscribe('per_image_analysis', self.per_image_analysis, acknowledgement=True)
    logging.getLogger('dials').setLevel(logging.INFO)

  def per_image_analysis(self, header, message):
    '''Run PIA on one image.'''

    # Conditionally acknowledge receipt of the message
    txn = self._transport.transaction_begin()
    self._transport.ack(header['message-id'], transaction=txn)

    # Extract the filename
    filename = message['file']

    filename = str(filename) # required due to
                             # https://github.com/dials/dials/issues/256

    self.log.info("Running PIA on %s", filename)

    # Do the per-image-analysis
    cl = ['d_max=40']
    results = work(filename, cl=cl)
    results['image'] = filename
    xml_response = response_to_xml(results)
    self.log.debug(str(results))
    self.log.debug(xml_response)

    # Send results onwards
    self._transport.send('transient.destination', results, transaction=txn)
    self._transport.transaction_commit(txn)
    self.log.info("PIA completed on %s", filename)
