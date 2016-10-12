from __future__ import absolute_import, division
import os
import time
from workflows.services.common_service import CommonService

from dials.command_line.find_spots_client import response_to_xml
from dials.command_line.find_spots_server import work

class DLSPerImageAnalysis(CommonService):
  '''A service that analyses individual images.'''

  # Human readable service name
  _service_name = "DLS Per-Image-Analysis"

  def initializing(self):
    '''Subscribe to the per_image_analysis queue. Received messages must be acknowledged.'''
    # TODO: Limit the number of messages in flight
    self._transport.subscribe('per_image_analysis', self.per_image_analysis, acknowledgement=True)

  def per_image_analysis(self, header, message):
    '''Run PIA on one image.'''

    #################
    # This is a bug in the message handling API.
    # The function should have receive a deserialized message.
    import json
    message = json.loads(message)
    #################

    # Conditionally acknowledge receipt of the message
    txn = self._transport.transaction_begin()
    self._transport.ack(header['message-id'], transaction=txn)

    # Extract the filename
    filename = message['file']
    print "Running PIA on", filename

    # Do the per-image-analysis
    cl = ['d_max=40']
    results = work(filename, cl=cl)
    results['image'] = filename
    xml_response = response_to_xml(results)
    print results
    print xml_response

    # Send results onwards
    self._transport.send('transient.destination', results, transaction=txn)
    self._transport.transaction_commit(txn)
    print "PIA completed on", filename
