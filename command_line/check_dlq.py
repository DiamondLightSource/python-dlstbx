from __future__ import absolute_import, division, print_function

import sys
import time
import uuid
from optparse import SUPPRESS_HELP, OptionParser

import workflows.transport

#
# dlstbx.check_dlq
#   Check number of messages in dead letter queues
#


class QueueStatus():
  '''Monitor ActiveMQ queue activity.'''

  # Dictionary of all known queues
  gather_interval = 5
  status = {}

  # Unique ID for this queue status monitor
  uuid = str(uuid.uuid4())
  report_queue = 'transient.qmonitor.' + uuid
  namespace = ''

  def __init__(self, transport=None, namespace=""):
    '''Set up monitor and connect to the network transport layer'''
    if transport is None or isinstance(transport, basestring):
      self._transport = workflows.transport.lookup(transport)()
    else:
      self._transport = transport()
    assert self._transport.connect(), "Could not connect to transport layer"
    self._transport.subscribe(self.report_queue, self.process_report, transformation=True)
    self.check_namespace = namespace
    if self.check_namespace and not self.check_namespace.endswith('.'):
      self.check_namespace = self.check_namespace + "."
    try:
      # Get namespace for introspection
      self.namespace = self._transport.get_namespace() + '.'
    except AttributeError:
      # use '' if get_namespace() is not offered by this transport method
      self.namespace = ''

  def run(self):
    self.last_answer = 0
    self.seen_answers = False
    self.dlq_messages = 0
    self.report = {}
    gather = time.time()

    self._transport.send('ActiveMQ.Statistics.Destination.DLQ.%s>' % self.check_namespace, '', headers = { 'JMSReplyTo': self.namespace + self.report_queue }, ignore_namespace=True )

    try:
      while time.time() < gather + 0.9 and not self.seen_answers:
        time.sleep(0.1)
      while self.seen_answers:
        self.seen_answers = False
        time.sleep(0.1)
    except KeyboardInterrupt:
      return

    if self.dlq_messages > 0:
      print("Total of %d DLQ messages found" % self.dlq_messages)
      sys.exit(1)
    else:
      print("No DLQ messages found")
      sys.exit(0)

  def process_report(self, header, message):
    self.seen_answers = True

    report = {}
    for entry in message['map']['entry']:
      if 'string' in entry:
        if isinstance(entry['string'], list):
          name = entry['string'].pop(0)
        else:
          name = entry['string']
          del(entry['string'])
      if len(entry) == 1:
        value_type = entry.iterkeys().next()
        report[name] = entry.itervalues().next()
        if value_type in ('long', 'int'):
          report[name] = int(report[name])
        if isinstance(report[name], list) and len(report[name]) == 1:
          report[name] = report[name][0]
      else:
        report[name] = entry
    destination = report.get('destinationName')
    if not destination: return
    if destination.endswith(self.report_queue): return

    if destination.startswith('queue://DLQ.'):
      if report.get('size'):
        print("DLQ for %s contains %d entries" % (destination.replace('queue://DLQ.',''), report['size']))
        self.dlq_messages += report['size']

if __name__ == '__main__':
  parser = OptionParser(
    usage='dlstbx.check_dlq [options]'
  )
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-n", "--namespace", dest="namespace",
      default="", help="Restrict check to this namespace")
  parser.add_option("-t", "--transport", dest="transport", metavar="TRN",
      default="stomp", help="Transport mechanism, default '%default'")

  # override default stomp host
  parser.add_option("--test", action="store_true", dest="test", help="Run in ActiveMQ testing (zocdev) namespace")
  default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'
  if '--test' in sys.argv:
    default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg'
  from workflows.transport.stomp_transport import StompTransport
  StompTransport.load_configuration_file(default_configuration)

  workflows.transport.add_command_line_options(parser)
  (options, args) = parser.parse_args()

  QueueStatus(transport=options.transport, namespace=options.namespace).run()
