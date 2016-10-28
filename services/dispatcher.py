from __future__ import absolute_import, division
import dlstbx
import json
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
    # TODO: Limit the number of messages in flight
    self._transport.subscribe('processing_recipe', self.process, acknowledgement=True)

  def process(self, header, message):
    '''Process an incoming processing request.'''

    # Conditionally acknowledge receipt of the message
    txn = self._transport.transaction_begin()
    self._transport.ack(header['message-id'], transaction=txn)

    # Process message
    print "Received processing request:\n" + str(message)
    if message.get('custom_recipe'):
        custom_recipe = workflows.recipe.Recipe(recipe=json.dumps(message['custom_recipe']))
        custom_recipe.validate()
        for destinationid, message in custom_recipe['start']:
          destination = custom_recipe[destinationid]
          headers = {}
          headers['recipe-pointer'] = destinationid
          headers['recipe'] = custom_recipe.serialize()
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
#      except workflows.WorkflowsError, e:
#        raise

    # Commit transaction
    self._transport.transaction_commit(txn)
    print "Processing completed"
