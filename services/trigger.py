from __future__ import absolute_import, division, print_function

import ispyb
import logging
import os

import six
import workflows.recipe
from procrunner import run_process
from workflows.services.common_service import CommonService

class DLSTrigger(CommonService):
  '''A service that creates and runs downstream processing jobs.
  '''

  # Human readable service name
  _service_name = "DLS Trigger"

  # Logger name
  _logger_name = 'dlstbx.services.trigger'

  def initializing(self):
    '''Subscribe to the trigger queue. Received messages must be acknowledged.'''
    workflows.recipe.wrap_subscribe(self._transport, 'trigger',
        self.trigger, acknowledgement=True, log_extender=self.extend_log)
    self.ispyb = ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')

  def trigger(self, rw, header, message):
    '''Forward the trigger message to a specific trigger function.'''
    # Extract trigger target from the recipe
    params = rw.recipe_step.get('parameters', {})
    target = params.get('target')
    if not target:
      self.log.error("No trigger target defined in recipe")
      rw.transport.nack(header)
      return
    if not hasattr(self, 'trigger_' + target):
      self.log.error("Unknown target %s defined in recipe", target)
      rw.transport.nack(header)
      return

    txn = rw.transport.transaction_begin()
    rw.set_default_channel('output')
    def parameters(parameter, replace_variables=True):
      if isinstance(message, dict):
        base_value = message.get(parameter, params.get(parameter))
      else:
        base_value = params.get(parameter)
      if not replace_variables or not base_value \
          or not isinstance(base_value, six.string_types) \
          or '$' not in base_value:
        return base_value
      for key in rw.environment:
        if '$' + key in base_value:
          base_value = base_value.replace('$' + key, str(rw.environment[key]))
      return base_value
    result = getattr(self, 'trigger_' + target)(
        rw=rw,
        header=header,
        message=message,
        parameters=parameters,
        transaction=txn,
    )
    if result and result.get('success'):
      rw.send({'result': result.get('return_value')}, transaction=txn)
      rw.transport.ack(header, transaction=txn)
    else:
      rw.transport.transaction_abort(txn)
      rw.transport.nack(header)
      return
    rw.transport.transaction_commit(txn)

  def trigger_dimple(self, rw, header, parameters, **kwargs):
    dcid = parameters('dcid')
    if not dcid:
      self.log.error('Dimple trigger failed: No DCID specified')
      return False

    import ispyb.model.__future__
    ispyb.model.__future__.enable('/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')

    dc_info = self.ispyb.get_data_collection(dcid)
    if not dc_info.pdb:
      self.log.info('Skipping dimple trigger: DCID has no associated PDB information')
      return {'success': True}

    dimple_parameters = {
        'data': parameters('mtz'),
        'scaling_id': parameters('scaling_id'),
    }

    jisp = self.ispyb.mx_processing.get_job_image_sweep_params()
    jisp['datacollectionid'] = dcid
    jisp['start_image'] = dc_info.image_start_number
    jisp['end_image'] = dc_info.image_start_number + dc_info.image_count - 1

    self.log.debug('Dimple trigger: Starting')

    jp = self.ispyb.mx_processing.get_job_params()
    jp['automatic'] = bool(parameters('automatic'))
    jp['comments'] = parameters('comment')
    jp['datacollectionid'] = dcid
    jp['display_name'] = "DIMPLE"
    jp['recipe'] = "postprocessing-dimple"
    jobid = self.ispyb.mx_processing.upsert_job(jp.values())
    self.log.debug('Dimple trigger: generated JobID {}'.format(jobid))

    for key, value in dimple_parameters.items():
      jpp = self.ispyb.mx_processing.get_job_parameter_params()
      jpp['job_id'] = jobid
      jpp['parameter_key'] = key
      jpp['parameter_value'] = value
      jppid = self.ispyb.mx_processing.upsert_job_parameter(jpp.values())
      self.log.debug('Dimple trigger: generated JobParameterID {}'.format(jppid))

    jisp['job_id'] = jobid
    jispid = self.ispyb.mx_processing.upsert_job_image_sweep(jisp.values())
    self.log.debug('Dimple trigger: generated JobImageSweepID {}'.format(jispid))

    self.log.debug('Dimple trigger: Processing job {} created'.format(jobid))

    message = { 'recipes': [], 'parameters': { 'ispyb_process': jobid } }
    rw.transport.send('processing_recipe', message)

    self.log.info('Dimple trigger: Processing job {} triggered'.format(jobid))

    return {'success': True, 'return_value': jobid}
