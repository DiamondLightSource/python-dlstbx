from __future__ import absolute_import, division
import dlstbx
import json
import os
import re
import time
import timeit
import uuid
from workflows.services.common_service import CommonService
import workflows.recipe

class DLSDispatcher(CommonService):
  '''Single point of contact service that takes in job meta-information
     (say, a data collection ID), a processing recipe, a list of recipes,
     or pointers to recipes stored elsewhere, and mangles these into something
     that can be processed by downstream services.
  '''

  # Human readable service name
  _service_name = "DLS Dispatcher"

  # Logger name
  _logger_name = 'dlstbx.services.dispatcher'

  # Store a copy of all dispatch messages in this location
  _logbook = '/dls/tmp/zocalo/dispatcher'

  def initializing(self):
    '''Subscribe to the processing_recipe queue. Received messages must be acknowledged.'''
    # self._environment.get('live') can be used to distinguish live/test mode
    self.log.info('Dispatcher starting')
    self.recipe_basepath = '/dls_sw/apps/zocalo/live/recipes'

    if self._environment.get('live'):
      try:
        os.makedirs(self._logbook, 0775)
      except OSError:
        pass # Ignore if exists
      if not os.access(self._logbook, os.R_OK | os.W_OK | os.X_OK):
        self.log.error('Logbook disabled: Can not write to location')
        self._logbook = None
    else:
      self.log.info('Logbook disabled: Not running in live mode')
      self._logbook = None

    self._transport.subscribe('processing_recipe', self.process, acknowledgement=True)

  def record_to_logbook(self, guid, header, message, recipewrap):
    basepath = os.path.join(self._logbook, time.strftime('%Y-%m-%d'))
    clean_guid = re.sub('[^a-z0-9A-Z\-]+', '', guid, re.UNICODE)
    if not clean_guid or len(clean_guid) < 3:
      self.log.warning('Message with non-conforming guid %s not written to logbook', guid)
      return
    try:
      os.makedirs(os.path.join(basepath, clean_guid[:2]))
    except OSError:
      pass # Ignore if exists
    try:
      log_entry = os.path.join(basepath, clean_guid[:2], clean_guid)
      with open(log_entry, 'w') as fh:
        fh.write('Incoming message header:\n')
        json.dump(header, fh, sort_keys=True, skipkeys=True, default=str,
                  indent=2, separators=(',', ': '))
        fh.write('\n\nIncoming message body:\n')
        json.dump(message, fh, sort_keys=True, skipkeys=True, default=str,
                  indent=2, separators=(',', ': '))
        fh.write('\n\nRecipe object:\n')
        json.dump(recipewrap.recipe.recipe, fh, sort_keys=True, skipkeys=True, default=str,
                  indent=2, separators=(',', ': '))
      self.log.debug('Message saved in logbook at %s', log_entry)
    except Exception:
      self.log.warning('Could not write message to logbook', exc_info=True)

  def process(self, header, message):
    '''Process an incoming processing request.'''
    # Time execution
    start_time = timeit.default_timer()

    # Load processing parameters
    parameters = message.get('parameters', {})
    if not isinstance(parameters, dict):
      # malformed message
      self.log.warning('Dispatcher rejected malformed message: parameters not given as dictionary')
      self._transport.nack(header)
      return

    # Conditionally acknowledge receipt of the message
    txn = self._transport.transaction_begin()
    self._transport.ack(header, transaction=txn)

    # Generate merged and individual recipe IDs if required.
    # 'guid' is a recipe-individual ID,
    # 'guid_merged' is identical across all recipes started at the same time,
    # and is attached to log records.
    # If 'guid' is already defined it overrides both.
    generate_individual_recipe_guids = not parameters.get('guid')
    recipe_id = parameters.get('guid') or str(uuid.uuid4())
    parameters['guid_merged'] = recipe_id

    # From here on add the global ID to all log messages
    with self.extend_log('recipe_ID', recipe_id):
      self.log.debug("Received processing request:\n" + str(message))
      self.log.debug("Received processing parameters:\n" + str(parameters))

      # At this point external helper functions should be called,
      # eg. ISPyB database lookups
      from dlstbx.ispybtbx import ispyb_filter
      message, parameters = ispyb_filter(message, parameters)
      self.log.debug("Mangled processing request:\n" + str(message))
      self.log.debug("Mangled processing parameters:\n" + str(parameters))

      # Process message
      recipes = []
      if message.get('custom_recipe'):
        recipes.append(workflows.recipe.Recipe(recipe=json.dumps(message['custom_recipe'])))
      if message.get('recipes'):
        for recipefile in message['recipes']:
          try:
            with open(os.path.join(self.recipe_basepath, recipefile + '.json'), 'r') as rcp:
              recipes.append(workflows.recipe.Recipe(recipe=rcp.read()))
          except ValueError, e:
            raise ValueError("Error reading recipe '%s': %s" % (recipefile, str(e)))

      full_recipe = workflows.recipe.Recipe()
      for recipe in recipes:
        recipe.validate()
        if generate_individual_recipe_guids:
          parameters['guid'] = str(uuid.uuid4())
        recipe.apply_parameters(parameters)
        full_recipe = full_recipe.merge(recipe)

      rw = workflows.recipe.RecipeWrapper(recipe=full_recipe, transport=self._transport)
      rw.environment = { 'ID': recipe_id } # FIXME: This should go into the constructor, but workflows can't do that yet
      rw.start(transaction=txn)

      # Write information to logbook if applicable
      if self._logbook:
        self.record_to_logbook(recipe_id, header, message, rw)

      # Commit transaction
      self._transport.transaction_commit(txn)
      self.log.info("Processed incoming message in %.4f seconds", timeit.default_timer() - start_time)
