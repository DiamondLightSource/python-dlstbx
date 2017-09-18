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
    self.ispybdbsp = driver(config_file='/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')
    self.log.info("ISPyB connector starting with ispyb v%s", ispyb.__version__)
    workflows.recipe.wrap_subscribe(
        self._transport, 'ispyb_connector', # will become 'ispyb' in far future
        self.ispyb_msg, acknowledgement=True, log_extender=self.extend_log)

  @staticmethod
  def parse_value(rw, parameter):
    base_value = rw.recipe_step['parameters'].get(parameter)
    if not base_value or '$' not in base_value:
      return base_value
    for key in rw.environment:
      if '$' + key in base_value:
        base_value = base_value.replace('$' + key, str(rw.environment[key]))
    return base_value

  def ispyb_msg(self, rw, header, message):
    '''Do something with ISPyB.'''

    command = rw.recipe_step['parameters'].get('ispyb_command')
    if not command:
      self.log.warning('Received message is not a valid ISPyB command')
      rw.transport.nack(header)
      return

    store_result = rw.recipe_step['parameters'].get('store_result')

    if command == 'update_processing_status':
      ppid = self.parse_value(rw, 'program_id')
      message = rw.recipe_step['parameters'].get('message')
      start_time = rw.recipe_step['parameters'].get('start_time')
      update_time = rw.recipe_step['parameters'].get('update_time')
      status = rw.recipe_step['parameters'].get('status')
      try:
        result = self.ispybdbsp.update_processing_status(
            ppid,
            status=status,
            start_time=start_time,
            update_time=update_time,
            update_message=message
          )
        self.log.info("Updating program %s status: '%s' with result %s", ppid, message, result)
      except ispyb.exception.ISPyBException as e:
        self.log.warning("Updating program %s status: '%s' caused exception '%s'.",
                         ppid, message, e, exc_info=True)
        result = None

    elif command == 'register_processing':
      program = rw.recipe_step['parameters'].get('program')
      cmdline = rw.recipe_step['parameters'].get('cmdline')
      environment = rw.recipe_step['parameters'].get('environment')
      if isinstance(environment, dict):
        environment = ', '.join('%s=%s' % (key, value) for key, value in environment.iteritems())
      rpid = rw.recipe_step['parameters'].get('rpid')
      try:
        result = self.ispybdbsp.add_processing_program(
                     reprocessing_id=rpid,
                     command_line=cmdline,
                     programs=program,
                     environment=environment)
        self.log.info("Registered new processing program '%s' for reprocessing id '%s' with command line '%s' and environment '%s' with result '%s'.",
                      program, rpid, cmdline, environment, result)
      except ispyb.exception.ISPyBException as e:
        self.log.warning("Registering new processing program '%s' for reprocessing id '%s' with command line '%s' and environment '%s' caused exception '%s'.",
                         program, rpid, cmdline, environment, e, exc_info=True)
        result = None

    else:
      self.log.warning('Received unknown ISPyB command (%s)',
                       command)
      rw.transport.nack(header)
      return

    if store_result:
      rw.environment[store_result] = result
      self.log.debug("Storing result '%s' in environment variable '%s'", result, store_result)

    txn = rw.transport.transaction_begin()
    rw.set_default_channel('output')
    rw.send({ 'result': result }, transaction=txn)
    rw.transport.ack(header, transaction=txn)
    rw.transport.transaction_commit(txn)
