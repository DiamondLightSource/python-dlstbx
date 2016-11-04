from __future__ import absolute_import, division
import dlstbx
import json
import os
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

  def initializing(self):
    '''Subscribe to the processing_recipe queue. Received messages must be acknowledged.'''
    self._transport.subscribe('processing_recipe', self.process, acknowledgement=True)

  def process(self, header, message):
    '''Process an incoming processing request.'''

    # Conditionally acknowledge receipt of the message
    txn = self._transport.transaction_begin()
    self._transport.ack(header['message-id'], transaction=txn)

    # Load processing parameters
    parameters = message.get('parameters', {})
    if 'guid' not in parameters:
      parameters['guid'] = str(uuid.uuid4())

    # At this point external helper functions should be called,
    # eg. ISPyB database lookups
    from dlstbx.ispyb.ispyb import ispyb_magic
    message, parameters = ispyb_magic(message, parameters)

    # Process message
    print "Received processing request:\n" + str(message)
    recipe = None
    if message.get('custom_recipe'):
      recipe = workflows.recipe.Recipe(recipe=json.dumps(message['custom_recipe']))
    if message.get('recipes'):
      recipefile = message['recipes'][0]
      with open(os.path.join('/dls_sw/apps/mx-scripts/plum-duff/recipes', recipefile + '.json'), 'r') as rcp:
        recipe = workflows.recipe.Recipe(recipe=rcp.read())
    if recipe:
        recipe.validate()
        recipe.apply_parameters(parameters)
        for destinationid, message in recipe['start']:
          destination = recipe[destinationid]
          headers = {}
          headers['recipe-pointer'] = destinationid
          headers['recipe'] = recipe.serialize()
          if destination.get('queue'):
            self._transport.send(destination['queue'],
                                 message,
                                 transaction=txn,
                                 headers=headers)
          if destination.get('topic'):
            self._transport.broadcast(destination['topic'],
                                      message,
                                      transaction=txn,
                                      headers=headers)

    # Commit transaction
    self._transport.transaction_commit(txn)
    print "Processing completed"
