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

    from dials.command_line.find_spots_client import response_to_xml
    from dials.command_line.find_spots_server import work

    cl = ['d_max=40']
    results = work(filename, cl=cl)
    results['image'] = filename
    xml_response = response_to_xml(results)
    print results
    print xml_response()

    self._transport.send('transient.destination', results)
    print "PIA completed on", filename
