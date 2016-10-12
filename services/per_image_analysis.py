from __future__ import absolute_import, division
import os
import time
from workflows.services.common_service import CommonService

class DLSPerImageAnalysis(CommonService):
  '''A service that analyses individual images.'''

  # Human readable service name
  _service_name = "DLS Per-Image-Analysis"

  def initializing(self):
    '''Subscribe to a channel.'''
    self._transport.subscribe('per_image_analysis', self.per_image_analysis)

  def per_image_analysis(self, header, message):
    '''Run PIA on one image.'''

    # This is a bug in the message handling API. This function should receive the deserialized message.
    import json
    message = json.loads(message)

    filename = message['file']
    print "Running PIA on", filename

    # Begin PIA-magic ============
    import time
    time.sleep(5)
    spot_count = 20
    resolution = 'magenta'
    strong_spots = 3
    # End PIA-magic ==============

    results = { 'file': filename,
                'spots': spot_count,
                'strong_spots': strong_spots,
                'resolution': resolution,
                'These are': 'per-image-analysis results', # not required
              }

    self._transport.send('transient.destination', results)
    print "PIA completed on", filename
