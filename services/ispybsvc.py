from __future__ import absolute_import, division, print_function

import os.path

import ispyb
import ispyb.factory
from dials.util.procrunner import run_process
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
       acknowledged. Prepare ISPyB database connection.'''
    self.log.info("ISPyB connector using ispyb v%s", ispyb.__version__)
    driver = ispyb.legacy_get_driver(ispyb.legacy_Backend.DATABASE_MYSQL)
    self.ispybdbsp = driver(config_file='/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')
    self.ispyb = ispyb.factory.create_connection('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')
    self.ispyb_mx = ispyb.factory.create_data_area(ispyb.factory.DataAreaType.MXPROCESSING, self.ispyb)
    self.log.debug("ISPyB connector starting")
    workflows.recipe.wrap_subscribe(
        self._transport, 'ispyb_connector', # will become 'ispyb' in far future
        self.receive_msg, acknowledgement=True, log_extender=self.extend_log,
        allow_non_recipe_messages=True)

  @staticmethod
  def parse_value(rw, message, parameter):
    if isinstance(message, dict):
      base_value = message.get(parameter, rw.recipe_step['parameters'].get(parameter))
    else:
      base_value = rw.recipe_step['parameters'].get(parameter)
    if not base_value or '$' not in base_value:
      return base_value
    for key in rw.environment:
      if '$' + key in base_value:
        base_value = base_value.replace('$' + key, str(rw.environment[key]))
    return base_value

  def receive_msg(self, rw, header, message):
    '''Do something with ISPyB.'''

    if not rw:
      # Incoming message is not a recipe message. Simple messages can be valid
      if not isinstance(message, dict) or not message.get('parameters') or not message.get('content'):
        self.log.warning('Rejected invalid simple message')
        self._transport.nack(header)
        return

      # Create a wrapper-like object that can be passed to functions
      # as if a recipe wrapper was present.
      class RW_mock(object):
        def dummy(self, *args, **kwargs):
          pass
      rw = RW_mock()
      rw.transport = self._transport
      rw.recipe_step = { 'parameters': message['parameters'] }
      rw.environment = {}
      rw.set_default_channel = rw.dummy
      rw.send = rw.dummy
      message = message['content']

    command = rw.recipe_step['parameters'].get('ispyb_command')
    if not command:
      self.log.warning('Received message is not a valid ISPyB command')
      rw.transport.nack(header)
      return
    if not hasattr(self, 'do_' + command):
      self.log.warning('Received unknown ISPyB command (%s)', command)
      rw.transport.nack(header)
      return

    txn = rw.transport.transaction_begin()
    rw.set_default_channel('output')
    result = getattr(self, 'do_' + command)(rw, message, txn)

    store_result = rw.recipe_step['parameters'].get('store_result')
    if store_result and result and 'return_value' in result:
      rw.environment[store_result] = result['return_value']
      self.log.debug("Storing result '%s' in environment variable '%s'", result['return_value'], store_result)
    if result and result.get('success'):
      rw.send({ 'result': result.get('return_value') }, transaction=txn)
      rw.transport.ack(header, transaction=txn)
#   elif rw.has_output_channel('error'):  # workflows does not support this atm
#     rw.send_to(...)
#     rw.transport.ack(header, transaction=txn)
    else:
      rw.transport.nack(header, transaction=txn)
    rw.transport.transaction_commit(txn)

  def do_update_processing_status(self, rw, message, txn):
    ppid = self.parse_value(rw, message, 'program_id')
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
      return { 'success': True, 'return_value': result }
    except ispyb.legacy.exception.ISPyBException as e:
      self.log.warning("Updating program %s status: '%s' caused exception '%s'.",
                       ppid, message, e, exc_info=True)
      return { 'success': False }

  def do_register_processing(self, rw, message, txn):
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
      return { 'success': True, 'return_value': result }
    except ispyb.legacy.exception.ISPyBException as e:
      self.log.warning("Registering new processing program '%s' for reprocessing id '%s' with command line '%s' and environment '%s' caused exception '%s'.",
                       program, rpid, cmdline, environment, e, exc_info=True)
      return { 'success': False }

  def do_add_program_attachment(self, rw, message, txn):
    params = self.ispyb_mx.get_program_attachment_params()
    params['parentid'] = self.parse_value(rw, message, 'program_id')
    params['file_name'] = message.get('file_name', rw.recipe_step['parameters'].get('file_name'))
    params['file_path'] = message.get('file_path', rw.recipe_step['parameters'].get('file_path'))

    if not os.path.isfile(params['file_path']):
      self.log.warning("Not adding attachment '%s' to data processing: File does not exist",
                       params['file_path'])
      return False

    params['file_type'] = str(message.get('file_type', rw.recipe_step['parameters'].get('file_type', ''))).lower()
    if params['file_type'] not in ('log', 'result', 'graph'):
      self.log.warning("Attachment type '%s' unknown, defaulting to 'log'", params['file_type'])
      params['file_type'] = 'log'

    self.log.debug("Writing program attachment to database: %s", params)

    result = self.ispyb_mx.upsert_program_attachment(list(params.values()))
    return { 'success': True, 'return_value': result }

  def do_add_datacollection_attachment(self, rw, message, txn):
    params = {}
    params['parentid'] = self.parse_value(rw, message, 'dcid')
    params['file_name'] = message.get('file_name', rw.recipe_step['parameters'].get('file_name'))
    params['file_path'] = message.get('file_path', rw.recipe_step['parameters'].get('file_path'))

    if not os.path.isfile(params['file_path']):
      self.log.warning("Not adding attachment '%s' to data collection: File does not exist",
                       params['file_path'])
      return False

    params['file_type'] = str(message.get('file_type', rw.recipe_step['parameters'].get('file_type', ''))).lower()
    if params['file_type'] not in ('snapshot', 'log', 'xy', 'recip'):
      self.log.warning("Attachment type '%s' unknown, defaulting to 'log'", params['file_type'])
      params['file_type'] = 'log'

    self.log.info("Not writing data collection attachment %s for DCID %s to database: Operation not supported" % \
                  (params['file_name'], params['parentid']))
#   self.log.debug("Writing data collection attachment to database: %s", params)

#   result = self.ispyb_mx.upsert_???_attachment(list(params.values()))
    result = 0

    return { 'success': True, 'return_value': result }

  def do_store_per_image_analysis_results(self, rw, message, txn):
    params = self.ispyb_mx.get_quality_indicators_params()

#   from pprint import pprint
#   pprint(message)

    params['datacollectionid'] = rw.recipe_step['parameters'].get('dcid')
    if not params['datacollectionid']:
      self.log.error('DataCollectionID missing from recipe')
      return { 'success': False }

    params['image_number'] = message.get('file-number')
    if not params['image_number']:
      self.log.error('Image number missing from message')
      return { 'success': False }

    params['dozor_score'] = message.get('dozor_score')
    params['spot_total'] = message.get('n_spots_total')
    if params['spot_total'] is not None:
      params['in_res_total'] = params['spot_total']
      params['icerings'] = 0
      params['maxunitcell'] = 0
      params['pctsaturationtop50peaks'] = 0
      params['inresolutionovrlspots'] = 0
      params['binpopcutoffmethod2res'] = 0
    elif params['dozor_score'] is None:
      self.log.error('Message contains neither dozor score nor spot count')
      return { 'success': False }

    params['totalintegratedsignal'] = message.get('total_intensity')
    params['good_bragg_candidates'] = message.get('n_spots_no_ice')
    params['method1_res'] = message.get('estimated_d_min')
    params['method2_res'] = message.get('estimated_d_min')

    params['programid'] = "65228265" # dummy value

    self.log.debug("Writing PIA results to database: %s", params)

    try:
#     result = "159956186" # for testing
      result = self.ispyb_mx.upsert_quality_indicators(list(params.values()))
      if rw.recipe_step['parameters'].get('notify-gda'):
        gdahost = rw.recipe_step['parameters']['notify-gda']
        if '{' in gdahost:
          self.log.warning('Could not notify GDA, %s is not a valid hostname', gdahost)
        elif gdahost == 'mx-control':
          pass # skip
        elif result is None:
          self.log.info('Could not notify GDA, stored procedure returned \'None\'')
        else:
          # now do mx-scripty notification of GDA
          try:
            udp_result = run_process(['python',
                                      '/dls_sw/apps/mx-scripts/misc/simple_udp.py',
                                      gdahost, '9876', 'ISPYB:ImageQualityIndicators,' + str(result)],
                                      timeout=5, print_stdout=False, print_stderr=False)
            if udp_result['exitcode'] != 0 or udp_result['timeout'] or udp_result['stdout'] == '' or udp_result['stderr'] != '':
              self.log.warning('GDA notification failed\n%s', udp_result)
            else:
              self.log.debug('GDA notification took %.2f seconds', udp_result['runtime'])
          except Exception as e:
            self.log.warning('Could not notify GDA: %s', e, exc_info=True)
    except ispyb.exception.ISPyBWriteFailed as e:
      self.log.error('Database says no: %s', e, exc_info=True)
      return { 'success': False }
    else:
      self.log.debug("PIA record %s written", result)
      return { 'success': True, 'return_value': result }
