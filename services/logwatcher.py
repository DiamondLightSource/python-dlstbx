from __future__ import absolute_import, division
from workflows.services.common_service import CommonService
from dlstbx.util.colorstreamhandler import ColorStreamHandler
import logging
import json
import sys
import time

class DLSLog(CommonService):
  '''Service showing log messages.'''

  # Human readable service name
  _service_name = "DLS Log Watcher"

  # Logger name
  _logger_name = "dlstbx.log"

  last_host = None
  last_host_messages = 0

  def initializing(self):
    '''Disable all irrelevant logging for this service.
       Then subscribe to log messages.'''
    logging.disable(logging.INFO)
    self._transport.subscribe_broadcast('transient.log', self.read_log_message)

    if hasattr(ColorStreamHandler, '_get_color'):
      def setbold():
        sys.stdout.write(ColorStreamHandler.BOLD)
      def setcolor(level):
        sys.stdout.write(getattr(ColorStreamHandler, '_get_color')(level))
      def resetcolor():
        sys.stdout.write(ColorStreamHandler.DEFAULT)
      self.setbold = setbold
      self.setcolor = setcolor
      self.resetcolor = resetcolor
    else:
      self.setbold = lambda: None
      self.setcolor = lambda x: None
      self.resetcolor = lambda: None

  def read_log_message(self, header, message):
    '''Process a log message'''
    if not isinstance(message, dict) or 'message' not in message:
      self.setcolor(logging.ERROR)
      print "=" * 80
      print "Unknown message:"
      print message
      print "=" * 80
      self.resetcolor()
    else:
      message['service_description'] = message.get('workflows_service', '')
      if 'workflows_statustext' in message:
        message['service_description'] = ' ({workflows_service}:{workflows_statustext})'.format(**message)
      message['workflows_host'] = message.get('workflows_host', '???')
      if message['workflows_host'] != self.last_host or self.last_host_messages > 20:
        self.last_host = message['workflows_host']
        self.last_host_messages = 0
        self.setbold()
        print "====== {workflows_host}{service_description} ======".format(**message)
        self.resetcolor()
      self.last_host_messages += 1
      self.setcolor(message.get('levelno', 0))
      if message['levelno'] >= logging.WARN:
        print "{pathname}:{lineno}{service_description}".format(**message)
      print "{name}: {msg}".format(**message)

#     print json.dumps(message, indent=2)
      self.resetcolor()
    time.sleep(0.1)
