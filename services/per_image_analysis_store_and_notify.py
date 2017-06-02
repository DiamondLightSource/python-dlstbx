from __future__ import absolute_import, division
from dials.command_line.find_spots_client import response_to_xml
from dials.util.procrunner import run_process
import logging
import workflows.recipe
from workflows.services.common_service import CommonService

class DLSPerImageAnalysisSAN(CommonService):
  '''A service that stores and notifies (S&N) for per-image-analysis results.'''

  # Human readable service name
  _service_name = "DLS PIA Store-and-Notify"

  # Logger name
  _logger_name = 'dlstbx.services.per_image_analysis_san'

  def initializing(self):
    '''Subscribe to the per_image_analysis queue. Received messages must be acknowledged.'''
    logging.getLogger('dials').setLevel(logging.INFO)
    workflows.recipe.wrap_subscribe(self._transport, 'per_image_analysis_san',
        self.store_and_notify, acknowledgement=True)

  def store_and_notify(self, rw, header, message):
    '''Store and Notify for PIA results.'''

    # Copy filename under 'image' key
    filename = message['image'] = message['file']

    self.log.debug("Storing PIA results for %s", filename)

    # Create XML from PIA result
    PIA_xml = response_to_xml(message)
    image_number = message['file-number']
    is_gridscan = rw.recipe_step.get('parameters', {}).get('gridscan') in ('True', 'true', 1)
    dcid = rw.recipe_step.get('parameters', {}).get('dcid', '')

    self.log.debug(PIA_xml)

    command = ['/bin/bash', '/dls_sw/apps/mx-scripts/misc/dials/imgScreen_LocalServerV2.sh',
               filename, 'NA', str(image_number), str(is_gridscan), str(dcid)]

    self.log.debug("Running %s", str(command))

    # Run bash script which stores and notifies for XML
    result = run_process(command, stdin=PIA_xml, print_stdout=True, print_stderr=True)

    if result['exitcode'] != 0:
      self.log.warn(result)
      # Reject message
      rw.transport.nack(header)
      self.log.warn("PIA results for %s could not be written to database", filename)
      return
    else:
      self.log.debug(result)

    # Begin transaction
    txn = rw.transport.transaction_begin()

    # Acknowledge message
    rw.transport.ack(header, transaction=txn)

    # Send results onwards
    rw.set_default_channel('result')
    rw.send_to('result', PIA_xml, transaction=txn)

    rw.transport.transaction_commit(txn)
    self.log.info("PIA results for %s written to database", filename)
