from __future__ import absolute_import, division, print_function

import logging
import os

import workflows.recipe
from procrunner import run_process
from workflows.services.common_service import CommonService

class DLSPerImageAnalysisSAN(CommonService):
  '''A service that stores and notifies (S&N) for per-image-analysis results.
     Well at least that is what it once did. We now do the storing and notifying properly via ISPyB.
     It still creates preview images for the first image of a data collection.
  '''

  # Human readable service name
  _service_name = "DLS PIA Store-and-Notify"

  # Logger name
  _logger_name = 'dlstbx.services.per_image_analysis_san'

  def initializing(self):
    '''Subscribe to the per_image_analysis queue. Received messages must be acknowledged.'''
    logging.getLogger('dials').setLevel(logging.INFO)
    workflows.recipe.wrap_subscribe(self._transport, 'per_image_analysis_san',
        self.store_and_notify, acknowledgement=True, log_extender=self.extend_log)

  def store_and_notify(self, rw, header, message):
    '''Store and Notify for PIA results.'''

    # Copy filename under 'image' key
    filename = message['image'] = message['file']

    self.log.debug("Storing PIA results for %s", filename)

    image_number = message['file-number'] # first image is always 1
    if str(image_number) != '1':
      self.log.warning("Not running on subsequent image %s", str(image_number))
      rw.transport.ack(header)
      return

    beamline = filename.split(os.path.sep)[2]
    if beamline == 'mx':
      self.log.debug("Not running on VMXi data")
      rw.transport.ack(header)
      return

    command = ['/bin/bash', '/dls_sw/apps/mx-scripts/bin/img2jpgv17-zocalo', filename]
    self.log.debug("Running %s", str(command))

    # Run bash script which stores and notifies for XML
    result = run_process(command, print_stdout=True, print_stderr=True)

    if result['exitcode'] != 0:
      self.log.warning("Could not run imgScreen on %s:\n%s", filename, str(result))
      # Reject message
      rw.transport.nack(header)
      return
    else:
      self.log.debug(str(result))

    # Acknowledge message
    rw.transport.ack(header)

    self.log.info("Successfully ran imgScreen on %s", filename)
