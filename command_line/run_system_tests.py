from __future__ import absolute_import, division

import dlstbx
import dlstbx.qa.result
import dlstbx.system_test
from dlstbx.util.colorstreamhandler import ColorStreamHandler
import logging
import sys
import time
from workflows.transport.stomp_transport import StompTransport

# Set up logging to console and graylog

logger = logging.getLogger('dlstbx')
console = ColorStreamHandler()
#if not debug:
#  console.setLevel(logging.INFO)
logger.addHandler(console)
logger.setLevel(logging.DEBUG)
dlstbx.enable_graylog()
logger = logging.getLogger('dlstbx.system_test')

# Set up transport

StompTransport.defaults['--stomp-host'] = 'ws154.diamond.ac.uk'
transport = StompTransport()
transport.connect()
if not transport.is_connected():
  logger.critical('Could not connect to ActiveMQ server')
  sys.exit(1)

# Load system tests

dlstbx.system_test.load_all_tests()
systest_classes = dlstbx.system_test.get_all_tests()
logger.info("Found %d system test classes" % len(systest_classes))

tests = {}
for classname, cls in systest_classes.iteritems():
  logger.debug('Collecting tests from %s' % classname)
  for testname, testsetting in cls().collect_tests().iteritems():
    testresult = dlstbx.qa.result.Result()
    testresult.set_name(testname)
    testresult.set_classname(classname)
    if testsetting.get('errors'):
      testresult.log_trace("\n".join(testsetting['errors']))
      testsetting['ignore'] = True
    tests[(classname, testname)] = (testsetting, testresult)
logger.info("Found %d system tests" % len(tests))

# Set up subscriptions

print("")

channels = {}
for test, _ in tests.itervalues():
  if not test.get('ignore'):
    for expectation in test['expect']:
      channels[(expectation['queue'], expectation['topic'])] = channels.get((expectation['queue'], expectation['topic']), [])
      channels[(expectation['queue'], expectation['topic'])].append(expectation)

channel_lookup = {}

def handle_receipt(header, message):
  expected_messages = channels[channel_lookup[header['subscription']]]
  for expected_message in expected_messages:
    if not expected_message.get('received'):
      if expected_message['message'] == message:
        if expected_message.get('headers'):
          headers_match = True
          for parameter, value in expected_message['headers'].iteritems():
            if value != header.get(parameter):
              headers_match = False
          if not headers_match:
            logger.warn("Received a message similar to an expected message:\n" + str(message) + "\n but its header\n" + str(header) + "\ndoes not match the expected header:\n" + str(expected_message['headers']))
            continue
        expected_message['received'] = True
        logger.debug("Received expected message:\n" + str(header) + "\n" + str(message) + "\n")
        return
  logger.warn("Received unexpected message:\n" + str(header) + "\n" + str(message) + "\n which is not in \n" + str(expected_messages) + "\n")

for queue, topic in channels.iterkeys():
  logger.debug("Subscribing to %s" % queue)
  if queue:
    sub_id = transport.subscribe(queue, handle_receipt)
  if topic:
    sub_id = transport.subscribe_broadcast(topic, handle_receipt)
  channel_lookup[str(sub_id)] = (queue, topic)

# Send out message

print("")

for test, _ in tests.itervalues():
  if not test.get('ignore'):
    for message in test['send']:
      if message.get('queue'):
        logger.debug("Sending message to %s", message['queue'])
        transport.send(message['queue'], message['message'], headers=message['headers'])
      if message.get('topic'):
        logger.debug("Broadcasting message to %s", message['topic'])
        transport.broadcast(message['topic'], message['message'], headers=message['headers'])

print("")

# Wait for messages and timeouts

start_time = time.time()
waiting = True
while waiting:
  print("Waited %5.1fs for messages." % (time.time() - start_time))
  time.sleep(0.2)
  waiting = False
  for testname, test in tests.iteritems():
    if not test[0].get('ignore'):
      for expectation in test[0]['expect']:
        if not expectation.get('received') and not expectation.get('received_timeout'):
          if time.time() > start_time + expectation['timeout']:
            expectation['received_timeout'] = True
            logger.warn("Test %s.%s timed out waiting for message\n%s" % (testname[0], testname[1], str(expectation)))
            test[1].log_error('No answer received within time limit.')
            test[1].log_error(str(expectation))
          else:
            waiting = True

# Export results
import junit_xml
ts = junit_xml.TestSuite("dlstbx.system_test",
                         [r for _, r in tests.itervalues()])
with open('output.xml', 'w') as f:
  junit_xml.TestSuite.to_file(f, [ts], prettyprint=True)

print("")

successes = sum(r.is_success() for _, r in tests.itervalues())
logger.info("System test run completed, %d of %d tests succeeded." % (successes, len(tests)))
for a, b in tests.itervalues():
  if not b.is_success():
    logger.warn("  %s %s received %d out of %d expected replies" % \
      (b.classname, b.name, len(filter(lambda x: x.get('received'), a['expect'])), len(a['expect'])))
