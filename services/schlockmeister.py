from __future__ import absolute_import, division
import uuid
from workflows.services.common_service import CommonService

class DLSSchlockmeister(CommonService):
  '''Remove too-often-redelivered messages from the queues.
     When a message is redelivered this can have inocuous reasons, for example
     when it was processed by a service that got killed by the user, or there
     was a temporary network glitch, or anything along those lines.
     In other cases the message may cause services to die without them catching
     the error. Schlockmeister subscribes to all queues and picks out messages
     that get redelivered too often and tells the broker to quarantine them.
     Redelivered messages may not be the cause of the issue, because messages
     can be prefetched by a client, which then dies for an unrelated reason,
     causing the messages to be redelivered. For this reason the redelivery
     limit is set rather high.

     schlockmeister
     a person who deals in or sells inferior or worthless goods; junk dealer.
  '''

  # Human readable service name
  _service_name = "DLS Schlockmeister"

  # Logger name
  _logger_name = 'dlstbx.services.schlockmeister'

  def initializing(self):
    '''Subscribe to all queues. Received messages must be acknowledged.
       Only receive messages that have been delivered 10 times in the past.
       Get messages without pre-processing, as this may have caused the crash.
    '''

    # Generate a unique ID in order to identify myself and other instances of
    # myself in the list of active subscribers/consumers
    self.uuid = str(uuid.uuid4())

    # Listen to a specific queue in the namespace. There will be no messages
    # in that queue. It only acts as a marker to identify service instances.
    self._markerqueue = 'transient.schlockmeister.' + self.uuid
    self._transport.subscribe(self._markerqueue, self.ignore, disable_mangling=True)

    self._transport.subscribe('>', self.quarantine, acknowledgement=True,
        selector="JMSXDeliveryCount>10", disable_mangling=True)
    self.log.info("Schlockmeister ready for work")

  def quarantine(self, header, message):
    '''Quarantine this message.'''

    self.log.warn("Schlockmeister has found a potentially bad message.\n" + \
                  "First 1000 characters of header:\n%s\n" + \
                  "First 1000 characters of message:\n%s",
                  str(header)[:1000], str(message)[:1000])

    # The actual quarantining magic happens on the broker
    self._transport.nack(header)

  @staticmethod
  def ignore(header, message):
    '''Ignore any messages on this subscription.'''
