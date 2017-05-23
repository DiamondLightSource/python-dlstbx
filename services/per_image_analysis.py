from __future__ import absolute_import, division
#from dials.command_line.find_spots_client import response_to_xml
from dials.command_line.find_spots_server import work
import logging
import os
import time
import workflows.recipe
from workflows.services.common_service import CommonService

class DLSPerImageAnalysis(CommonService):
  '''A service that analyses individual images.'''

  # Human readable service name
  _service_name = "DLS Per-Image-Analysis"

  # Logger name
  _logger_name = 'dlstbx.services.per_image_analysis'

  def initializing(self):
    '''Subscribe to the per_image_analysis queue. Received messages must be acknowledged.'''
    logging.getLogger('dials').setLevel(logging.INFO)
    workflows.recipe.wrap_subscribe(self._transport, 'per_image_analysis',
        self.per_image_analysis, acknowledgement=True)

  def per_image_analysis(self, recipe, header, message):
    '''Run PIA on one image.'''

    # Only process messages in workflows recipe format
    if not recipe:
      self.log.error("Rejecting message not in recipe format:\n%s" % str(header))
      self._transport.nack(header)
      return

    # Conditionally acknowledge receipt of the message
    txn = recipe.transport.transaction_begin()
    recipe.transport.ack(header, transaction=txn)

    # Extract the filename
    filename = message['file']

    filename = str(filename) # required due to
                             # https://github.com/dials/dials/issues/256

    self.log.info("Running PIA on %s", filename)

    # Do the per-image-analysis
    cl = ['d_max=40']
    results = work(filename, cl=cl)
    results['image'] = filename
 #   xml_response = response_to_xml(results)
    self.log.debug(str(results))
 #   self.log.debug(xml_response)

    # Send results onwards
    recipe.send_to('result', results, use_default_channel=True)
    self._transport.send('transient.destination', results, transaction=txn)
    self._transport.transaction_commit(txn)
    self.log.info("PIA completed on %s", filename)

    # nonsense


    workflows.recipe.wrap_subscribe(self._transport, 'per_image_analysis',
       self.per_image_analysis, acknowledgement=True,
       default_output_channel = 'result')

    workflows.recipe.wrap_subscribe(self._transport, 'per_image_analysis',
       self.per_image_analysis, acknowledgement=True,
       wrapper_arguments = { 'default_output_channel': 'result' })

    workflows.recipe.wrap_subscribe(self._transport, 'per_image_analysis',
       self.per_image_analysis, acknowledgement=True,
       default_output_channel = 'result')


    recipe.unnamed_output_channel = 'result'
    recipe.send_to('result', result)
    recipe.send_error(result)

