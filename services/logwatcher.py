from __future__ import absolute_import, division
from workflows.services.common_service import CommonService
import json
import time

class DLSLog(CommonService):
  '''Service showing log messages.'''

  # Human readable service name
  _service_name = "DLS Log Watcher"

  # Logger name
  _logger_name = "dlstbx.log"

  def initializing(self):
    '''Subscribe to log messages.'''
    self._transport.subscribe_broadcast('transient.log', self.read_log_message)

  def read_log_message(self, header, message):
    '''Consume a message'''
    logmessage = { 'time': (time.time() % 1000) * 1000,
                   'header': '',
                   'message': message }
    if header:
      logmessage['header'] = json.dumps(header, indent=2) + '\n' + \
                             '----------------' + '\n'

    print "=== Consume ====\n{header}{message}\n========Received@{time}".format(**logmessage)
    time.sleep(0.1)
