from __future__ import absolute_import, division, print_function

import os.path
import pprint

import workflows.recipe
from libtbx.procrunner import run_process
from workflows.services.common_service import CommonService

class DLSMailer(CommonService):
  '''A service that generates emails from messages.'''

  # Human readable service name
  _service_name = "DLS Mail Notifications"

  # Logger name
  _logger_name = 'dlstbx.services.mailer'

  def initializing(self):
    '''Subscribe to the Mail notification queue.
       Received messages must be acknowledged.'''
    self.log.debug("Mail notifications starting")
    workflows.recipe.wrap_subscribe(
        self._transport, 'mailnotification',
        self.receive_msg, acknowledgement=True, log_extender=self.extend_log,
        allow_non_recipe_messages=True)

  def receive_msg(self, rw, header, message):
    '''Do some mail notification.'''

    if rw:
      parameters = rw.recipe_step['parameters']
      content = None
    else:
      # Incoming message is not a recipe message. Simple messages can be valid
      if not isinstance(message, dict) or not message.get('parameters') or not message.get('content'):
        self.log.warning('Rejected invalid simple message')
        self._transport.nack(header)
        return

      parameters = message['parameters']
      content = message['content']

    recipients = parameters.get('recipients', parameters.get('recipient'))
    if not recipients:
      self.log.warning('No recipients set for message')
      self._transport.nack(header)
      return
    if isinstance(recipients, list):
      pass
    elif isinstance(recipients, tuple):
      recipients = list(recipients)
    else:
      recipients = [recipients]

    sender = parameters.get('from', 'Zocalo <zocalo@diamond.ac.uk>')

    subject = parameters.get('subject', 'mail notification via zocalo')

    content = parameters.get('content', content)
    if not content:
      self.log.warning('Message has no content')
      self._transport.nack(header)
      return
    if isinstance(content, list):
      content = ''.join(content)

    if isinstance(message, list) and all(isinstance(line, unicode) for line in message):
      pprint_message = "\n".join(line.encode('ascii', errors='replace') for line in message)
    else:
      pprint_message = pprint.pformat(message)

    content = content.format(payload=message, pprint_payload=pprint_message)

    sender = sender.encode('ascii', errors='replace')
    subject = subject.encode('ascii', errors='replace')
    recipients = [ r.encode('ascii', errors='replace') for r in recipients ]
    content = content.encode('ascii', errors='replace')

    self.log.info("Sending mail notification %r to %r", subject, recipients)

    # Accept message before sending mail. While this means we do not guarantee
    # message delivery it also means if the service crashes after delivery we
    # will not re-deliver the message inifinitely many times.
    self._transport.ack(header)

    result = run_process(
        ['/bin/mail', '-s', subject] + recipients,
        environment_override={'from': sender},
        print_stderr=False,
        print_stdout=False,
        stdin=content,
        timeout=60,
    )

    if result['exitcode'] or result['stderr']:
      self.log.error('Message delivery failed with exit code %r: %r', result['exitcode'], result['stdout'] + result['stderr'])
    elif result['timeout']:
      self.log.error('Message delivery failed with timeout: %r', result['stdout'] + result['stderr'])
    elif result['stdin_bytes_remain']:
      self.log.error('Message delivery failed with %d bytes unread: %r', result['stdin_bytes_remain'], result['stdout'] + result['stderr'])
    else:
      self.log.debug('Message sent successfully')
