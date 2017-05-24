from __future__ import absolute_import, division
import dlstbx
import json
import os
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

  def initializing(self):
    '''Subscribe to the processing_recipe queue. Received messages must be acknowledged.'''
    if self._environment.get('live'):
      self.log.info('Dispatcher starting in live mode')
      self.recipe_basepath = '/dls_sw/apps/zocalo/live/recipes'
    else:
      self.log.info('Dispatcher starting in testing mode')
      self.recipe_basepath = '/dls_sw/apps/zocalo/test/recipes'
    self._transport.subscribe('processing_recipe', self.process, acknowledgement=True)

  def process(self, header, message):
    '''Process an incoming processing request.'''
    # Time execution
    start_time = timeit.default_timer()

    # Conditionally acknowledge receipt of the message
    txn = self._transport.transaction_begin()
    self._transport.ack(header, transaction=txn)

    # Load processing parameters
    parameters = message.get('parameters', {})
    generate_guids = 'guid' not in parameters

    self.log.debug("Received processing request:\n" + str(message))
    self.log.debug("Received processing parameters:\n" + str(parameters))

    # At this point external helper functions should be called,
    # eg. ISPyB database lookups
    from dlstbx.ispyb.ispyb import ispyb_filter
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
      if generate_guids:
        parameters['guid'] = str(uuid.uuid4())
      recipe.apply_parameters(parameters)
      full_recipe = full_recipe.merge(recipe)

    rw = workflows.recipe.RecipeWrapper(recipe=full_recipe, transport=self._transport)
    rw.start(transaction=txn)

    # Commit transaction
    self._transport.transaction_commit(txn)
    self.log.info("Processed incoming message in %.4f seconds", timeit.default_timer() - start_time)
