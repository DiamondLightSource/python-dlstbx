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

    # Conditionally acknowledge receipt of the message
    txn = rw.transport.transaction_begin()
    rw.transport.ack(header, transaction=txn)

    # Extract the filename
    filename = message['file']

    filename = str(filename) # required due to
                             # https://github.com/dials/dials/issues/256

    self.log.info("Running PIA on %s", filename)

    # Create XML from PIA result
    PIA_xml = response_to_xml(message)
    self.log.debug(PIA_xml)

    # Run bash script which stores and notifies for XML
    result = run_process(['/bin/bash', '/dls_sw/apps/mx-scripts/misc/dials/imgScreen_LocalServerV2.sh',
      filename, 'NA', str(kwargs['imagenumber']), str(kwargs['grid']), kwargs['dcid']],
      stdin=PIA_xml, debug=True, print_stdout=True)
    if result['exitcode'] != 0:
      info(result)
    self.log.debug("%d: Thread stopped after %.1f seconds" % (pid, timeit.default_timer() - start))

    # Send results onwards
    rw.set_default_channel('result')
    rw.send_to('result', PIA_xml, transaction=txn)
    rw.transport.transaction_commit(txn)
    self.log.info("PIA completed on %s", filename)
