from __future__ import absolute_import, division
import ispyb
import workflows.recipe
from workflows.services.common_service import CommonService

class DLSISPyB(CommonService):
  '''A service that receives information to be written to ISPyB.'''

  # Human readable service name
  _service_name = "DLS ISPyB connector"

  # Logger name
  _logger_name = 'dlstbx.services.ispyb'

  def initializing(self):
    '''Subscribe the ISPyB connector queue. Received messages must be
       acknowledged.'''
    driver = ispyb.get_driver(ispyb.Backend.DATABASE_MYSQL)
    self.ispybdb = driver(config_file='/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
    self.log.info("ISPyB connector starting with ispyb v%s", ispyb.__version__)
    workflows.recipe.wrap_subscribe(
        self._transport, 'ispyb_connector', # will become 'ispyb' in far future
        self.ispyb_msg, acknowledgement=True, log_extender=self.extend_log)

  def ispyb_msg(self, rw, header, message):
    '''Do something with ISPyB.'''

    if not isinstance(message, dict) or not message.get('ispyb_command'):
      self.log.warning('Received message is not a valid ISPyB command')
      rw.transport.nack(header)
      return

    if message['ispyb_command'] == 'update_processing_status':
      self.log.debug('Updating processing status')
    else:
      self.log.warning('Received unknown ISPyB command (%s)',
                       message['ispyb_command'])
      rw.transport.nack(header)
      return

    self.log.debug(str(message))
    rw.transport.ack(header)
