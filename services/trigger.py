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

  def trigger(self, rw, header, message):
    '''Forward the trigger message to a specific trigger function.'''
    # Extract trigger target from the recipe
    params = rw.recipe_step.get('parameters', {})
    target = params.get('target')
    if not target:
      self.log.error("No trigger target defined in recipe")
      rw.transport.nack(header)
      return
    if not hasattr(self, 'trigger_' + target)
      self.log.error("Unknown target %s defined in recipe", target)
      rw.transport.nack(header)
      return

    txn = rw.transport.transaction_begin()
    rw.set_default_channel('output')
    def parameters(parameter, replace_variables=True):
      if isinstance(message, dict):
        base_value = message.get(parameter, parms.get(parameter))
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

  @staticmethod
  def dimple_has_matching_pdb(dcid):
    with ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg') as i:
      import ispyb.model.__future__
      ispyb.model.__future__.enable('/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
      for pdb in i.get_data_collection(dcid).pdb:
        if pdb.code is not None:
          return True
        elif pdb.rawfile is not None:
          assert pdb.name is not None
          return True
      return False

  def trigger_dimple(self, rw, header, parameters):
    dcid = parameters.get('dcid')
    if not dcid:
      self.log.error('Dimple trigger failed: No DCID specified')
      return False
    if not self.dimple_has_matching_pdb(dcid):
      self.log.info('Skipping dimple trigger: DCID has no associated PDB information')
      return {'success': True}
    self.log.warn('Triggering dimple')
