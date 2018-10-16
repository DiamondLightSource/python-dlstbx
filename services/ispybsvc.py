from __future__ import absolute_import, division, print_function

import os.path
import time

import dlstbx.util.gda
import ispyb
import ispyb.exception
import mysql.connector
import workflows.recipe
from procrunner import run_process
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
    self.ispyb = ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')
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

    if header.get('redelivered') == 'true':
      # A redelivered message may just have been processed in a parallel instance,
      # which was connected to a different database server in the DB cluster. If
      # we were to process it immediately we may run into a DB synchronization
      # fault. Avoid this by giving the DB cluster a bit of time to settle.
      self.log.debug('Received redelivered message, holding for a second.')
      time.sleep(1)

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
      result = self.ispyb.mx_processing.upsert_program_ex(
          program_id=ppid,
          status={'success':1, 'failure':0}.get(status),
          time_start=start_time,
          time_update=update_time,
          message=message
        )
      self.log.info("Updating program %s status: '%s' with result %s", ppid, message, result)
      return { 'success': True, 'return_value': result }
    except ispyb.exception.ISPyBException as e:
      self.log.warning("Updating program %s status: '%s' caused exception '%s'.",
                       ppid, message, e, exc_info=True)
      return { 'success': False }

  def do_store_dimple_failure(self, rw, message, txn):
    params = self.ispyb.mx_processing.get_run_params()
    params['parentid'] = self.parse_value(rw, message, 'scaling_id')
    params['pipeline'] = 'dimple'
    params['success'] = 0
    params['message'] = 'Unknown error'
    params['run_dir'] = self.parse_value(rw, message, 'directory')
    try:
      result = self.ispyb.mx_processing.upsert_run(params.values())
      return { 'success': True, 'return_value': result }
    except ispyb.exception.ISPyBException as e:
      self.log.warning("Updating DIMPLE failure for %s caused exception '%s'.",
                       params['parentid'], e, exc_info=True)
      return { 'success': False }

  def do_register_processing(self, rw, message, txn):
    program = rw.recipe_step['parameters'].get('program')
    cmdline = rw.recipe_step['parameters'].get('cmdline')
    environment = rw.recipe_step['parameters'].get('environment')
    if isinstance(environment, dict):
      environment = ', '.join('%s=%s' % (key, value) for key, value in environment.iteritems())
    rpid = rw.recipe_step['parameters'].get('rpid')
    try:
      result = self.ispyb.mx_processing.upsert_program_ex(
          job_id=rpid,
          name=program,
          command=cmdline,
          environment=environment,
      )
      self.log.info("Registered new program '%s' for processing id '%s' with command line '%s' and environment '%s' with result '%s'.",
                    program, rpid, cmdline, environment, result)
      return { 'success': True, 'return_value': result }
    except ispyb.exception.ISPyBException as e:
      self.log.warning("Registering new program '%s' for processing id '%s' with command line '%s' and environment '%s' caused exception '%s'.",
                       program, rpid, cmdline, environment, e, exc_info=True)
      return { 'success': False }

  def do_add_program_attachment(self, rw, message, txn):
    params = self.ispyb.mx_processing.get_program_attachment_params()
    params['parentid'] = self.parse_value(rw, message, 'program_id')
    params['file_name'] = message.get('file_name', rw.recipe_step['parameters'].get('file_name'))
    params['file_path'] = message.get('file_path', rw.recipe_step['parameters'].get('file_path'))
    fqpn = os.path.join(params['file_path'], params['file_name'])

    if not os.path.isfile(fqpn):
      self.log.warning("Not adding attachment '%s' to data processing: File does not exist", str(fqpn))
      return False

    params['file_type'] = str(message.get('file_type', rw.recipe_step['parameters'].get('file_type', ''))).lower()
    if params['file_type'] not in ('log', 'result', 'graph'):
      self.log.warning("Attachment type '%s' unknown, defaulting to 'log'", params['file_type'])
      params['file_type'] = 'log'

    self.log.debug("Writing program attachment to database: %s", params)

    result = self.ispyb.mx_processing.upsert_program_attachment(list(params.values()))
    return { 'success': True, 'return_value': result }

  def do_add_datacollection_attachment(self, rw, message, txn):
    params = self.ispyb.mx_acquisition.get_data_collection_file_attachment_params()

    params['parentid'] = self.parse_value(rw, message, 'dcid')
    file_name = message.get('file_name', rw.recipe_step['parameters'].get('file_name'))
    file_path = message.get('file_path', rw.recipe_step['parameters'].get('file_path'))
    params['file_full_path'] = os.path.join(file_path, file_name)

    if not os.path.isfile(params['file_full_path']):
      self.log.warning("Not adding attachment '%s' to data collection: File does not exist", str(params['file_full_path']))
      return False

    params['file_type'] = str(message.get('file_type', rw.recipe_step['parameters'].get('file_type', ''))).lower()
    if params['file_type'] not in ('snapshot', 'log', 'xy', 'recip', 'pia'):
      self.log.warning("Attachment type '%s' unknown, defaulting to 'log'", params['file_type'])
      params['file_type'] = 'log'

    self.log.debug("Writing data collection attachment to database: %s", params)
    result = self.ispyb.mx_acquisition.upsert_data_collection_file_attachment(list(params.values()))

    return { 'success': True, 'return_value': result }

  def do_store_per_image_analysis_results(self, rw, message, txn):
    params = self.ispyb.mx_processing.get_quality_indicators_params()

#   from pprint import pprint
#   pprint(message)

    params['datacollectionid'] = rw.recipe_step['parameters'].get('dcid')
    if not params['datacollectionid']:
      self.log.error('DataCollectionID missing from recipe')
      return { 'success': False }

    params['image_number'] = message.get('file-pattern-index', message.get('file-number'))
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

    self.log.debug("Writing PIA record for image %r in DCID %s", params['image_number'], params['datacollectionid'])

    try:
#     result = "159956186" # for testing
      result = self._retry_mysql_call(self.ispyb.mx_processing.upsert_quality_indicators, list(params.values()))
      if rw.recipe_step['parameters'].get('notify-gda'):
        gdahost = rw.recipe_step['parameters']['notify-gda']
        if '{' in gdahost:
          self.log.warning('Could not notify GDA, %s is not a valid hostname', gdahost)
        elif gdahost == 'mx-control':
          pass # skip
        elif result is None:
          self.log.info('Could not notify GDA, stored procedure returned \'None\'')
        else:
          # We still notify in the legacy manner by sending a UDP package with DCID+imagenumber
          try:
            dlstbx.util.gda.notify(gdahost, 9877, "IQI:{p[datacollectionid]}:{p[image_number]}".format(p=params))
          except Exception as e:
            self.log.warning('Could not notify GDA: %s', e, exc_info=True)
    except ispyb.exception.ISPyBWriteFailed as e:
      self.log.error("Could not write PIA results %s to database: %s", params, e, exc_info=True)
      return { 'success': False }
    else:
      return { 'success': True, 'return_value': result }

  def do_insert_alignment_result(self, rw, message, txn):
    try:
      program = message.get('program', '')
      chi = message.get('chi')
      phi = message.get('phi')
      kappa = message.get('kappa')

      assert phi is not None
      assert [chi, kappa].count(None) == 1

      mx_screening = self.ispyb.mx_screening
      screening_params = mx_screening.get_screening_params()

      screening_params['dcid'] = message['dataCollectionId']
      screening_params['program_version'] = program
      screening_params['comments'] = message.get('comments', '')
      screening_params['short_comments'] = message.get('shortComments', '')

      screeningId = mx_screening.insert_screening(list(screening_params.values()))
      assert screeningId is not None

      output_params = mx_screening.get_screening_output_params()
      output_params['screening_id'] = screeningId
      screeningOutputId = mx_screening.insert_screening_output(list(output_params.values()))
      assert screeningOutputId is not None

      strategy_params = mx_screening.get_screening_strategy_params()
      strategy_params['screening_output_id'] = screeningOutputId
      strategy_params['program'] = program
      screeningStrategyId = mx_screening.insert_screening_strategy(list(strategy_params.values()))
      assert screeningStrategyId is not None

      wedge_params = mx_screening.get_screening_strategy_wedge_params()
      wedge_params['screening_strategy_id'] = screeningStrategyId
      wedge_params['chi'] = chi
      wedge_params['kappa'] = kappa
      wedge_params['phi'] = phi
      screeningStrategyWedgeId = mx_screening.insert_screening_strategy_wedge(list(wedge_params.values()))
      assert screeningStrategyWedgeId is not None

      self.log.info("Inserted alignment results with IDs %s, %s, %s, %s",
          str(screeningId), str(screeningOutputId), str(screeningStrategyId), str(screeningStrategyWedgeId))
      return { 'success': True }
    except (ispyb.exception.ISPyBException, AssertionError) as e:
      self.log.warning("Inserting alignment results: '%s' caused exception '%s'.",
                       message, e, exc_info=True)
      return { 'success': False }

  def _retry_mysql_call(self, function, *args, **kwargs):
    tries = 0
    while True:
      try:
        return function(*args, **kwargs)
      except mysql.connector.errors.InternalError as e:
        tries = tries + 1
        if tries < 3:
          self.log.warning("ISPyB call %s try %d failed with %s", function, tries, e, exc_info=True)
          continue
        else:
          raise
