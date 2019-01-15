from __future__ import absolute_import, division, print_function

import os.path
import time

import dlstbx.util.gda
import mysql.connector
import six
import workflows.recipe
from workflows.services.common_service import CommonService

class DLSNotifyGDA(CommonService):
  '''A service that forwards PIA results to GDA.'''

  # Human readable service name
  _service_name = "DLS GDA Bridge"

  # Logger name
  _logger_name = 'dlstbx.services.notifygda'

  def initializing(self):
    """
    Subscribe to the GDA notification queue. Received messages must be
    acknowledged.
    """
    self.log.debug("GDA Bridge starting")
    workflows.recipe.wrap_subscribe(
        self._transport, 'notify_gda', # consider transient queue
        self.notify_gda, acknowledgement=True, log_extender=self.extend_log)

  def notify_gda(self, rw, header, message):
    '''Forward some information to GDA.'''

    if not isinstance(message, dict):
      self.log.error('message payload must be a dictionary')
      rw.transport.nack(header)
      return

    parameter = rw.recipe_step['parameters'].get
    def message_or_parameter(key):
      return message.get(key, parameter(key))

    dcid = parameter('dcid')
    if not dcid:
      self.log.error('DataCollectionID not specified')
      rw.transport.nack(header)
      return

    image_number = message_or_parameter('file-pattern-index') or message_or_parameter('file-number')
    if not image_number:
      self.log.error('Image number not specified')
      rw.transport.nack(header)
      return

    number_of_spots = message_or_parameter('n_spots_total')
    if number_of_spots is None:
      self.log.error('Message does not contain a spot count')
      rw.transport.nack(header)
      return

    params = {}
    params['totalintegratedsignal'] = message_or_parameter('total_intensity')
    params['good_bragg_candidates'] = message_or_parameter('n_spots_no_ice')
    params['method1_res'] = message_or_parameter('estimated_d_min')
    # what are we interested in?

    value = number_of_spots

    self.log.debug("Forwarding PIA record for image %r in DCID %s", image_number, dcid)

    gdahost = parameter('host')
    gdaport = parameter('port')
    if not gdahost or not gdaport:
      self.log.error('GDA host/port undefined')
      rw.transport.nack(header)
      return
    if '{' in gdahost:
      self.log.error('Could not notify GDA, %s is not a valid hostname', gdahost)
      rw.transport.nack(header)
      return
    elif gdahost == 'mx-control':
      pass # skip
    else:
      # We notify according to MXGDA-3243 by sending a UDP package with DCID+imagenumber+????
      try:
        dlstbx.util.gda.notify(gdahost, gdaport, "IQI:{dcid}:{image_number}:{value}".format(dcid=dcid, image_number=image_number, value=value))
      except Exception as e:
        self.log.error('Could not notify GDA: %s', e, exc_info=True)
        rw.transport.nack(header)
        return
    rw.transport.ack(header)
