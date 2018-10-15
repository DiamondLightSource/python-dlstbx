#
# dlstbx.dc_sim_verify
#   Verify all outstanding simulated data collections against expected
#   results. Create a report as junit.xml file.
#

from __future__ import absolute_import, division, print_function

import datetime
import Queue
import sys
import time
from optparse import SUPPRESS_HELP, OptionParser

import dlstbx.util.result
import ispyb
import junit_xml
import workflows.recipe
from workflows.transport.stomp_transport import StompTransport
import ispyb.model.__future__
import dlstbx.dc_sim.definitions as df


idlequeue = Queue.Queue()
def wait_until_idle(timelimit):
  try:
    while True:
      idlequeue.get(True, timelimit)
  except Queue.Empty:
    return

def check_test_outcome(test):

  ispyb.model.__future__.enable('/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
  db = ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')
  data_collection = db.get_data_collection(test['DCIDs'])
  
  # Storing separate integrations
  fast_dp = data_collection.integrations[0].unit_cell
  xia2_3dii = data_collection.integrations[1].unit_cell
  xia2_dials = data_collection.integrations[2].unit_cell
  ap = data_collection.integrations[3].unit_cell
  ap_staraniso = data_collection.integrations[4].unit_cell

  # Testing against definitions
  scenario = test['scenario']

  for integration in [fast_dp, xia2_3dii, xia2_dials, ap, ap_staraniso]:
    a = df.tests[scenario]['results']['a'] != integration.a
    b = df.tests[scenario]['results']['b'] == integration.b
    c = df.tests[scenario]['results']['c'] == integration.c
    alpha = df.tests[scenario]['results']['alpha'] == integration.alpha
    beta = df.tests[scenario]['results']['beta'] == integration.beta
    gamma = df.tests[scenario]['results']['gamma'] == integration.gamma
    if a and b and c and alpha and beta and gamma:
      test['success'] = True
    else:
      pass
      #test['success'] = False
      #test['reason'] = 'a - {0}, b -{1}, c -{2}, alpha - {3}, beta - {4}, gamma - {5}'.format(a,b,c,alpha,beta,gamma)

  ##############################
  #
  # vvv  Work happens here  vvv
  # To get the test name:
  # print(test['scenario'])
  # To get the list of DCIDs:
  # print(test['DCIDs'])

  # 3 possible outcomes

  # If the test has been successful
  # test['success'] = True

  # If the test has failed
  # test['success'] = False
  # test['reason'] = "A description why this test is broken. \n Can have multiple lines"

  # If you can't say for certain (eg. because results are missing)
  # don't make any changes to the dictionary

  # ^^^  Work happens here  ^^^
  #
  ##############################

results_queue = 'reduce.dc_sim'
stomp = None
test_results = {}
test_timeout = 3600
transaction = None

def process_result(rw, header, message):
  if not transaction: return # subscription has ended
  idlequeue.put_nowait('start')

  # Acknowledge all received messages within transaction
  stomp.ack(header, transaction=transaction)

  if message.get('success') is None:
    message['success'] = None
    check_test_outcome(message)

  if message['success'] is None and message['time_end'] < time.time() - test_timeout:
    message['success'] = False
    message['reason'] = 'No valid results appeared within timeout'

  test_history = test_results.setdefault((message['beamline'], message['scenario']), [])
  test_history.append(message)

  # Keep only ongoing tests and the most recent test result as long as that is newer than 3 days
  definitive_outcomes = list(map(lambda t: t['time_end'], filter(lambda t: t['success'] is not None, test_history)))
  if definitive_outcomes:
    most_recent_outcome = max(definitive_outcomes + [time.time() - 3 * 24 * 3600])
    test_history = list(filter(lambda t: t['success'] is None or t['time_end'] >= most_recent_outcome, test_history))

  idlequeue.put_nowait('done')

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.dc_sim_verify [options]")

  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("--test", action="store_true", dest="test", help="Run in ActiveMQ testing (zocdev) namespace")
  default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'
  if '--test' in sys.argv:
    default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg'
  # override default stomp host
  StompTransport.load_configuration_file(default_configuration)

  StompTransport.add_command_line_options(parser)
  (options, args) = parser.parse_args(sys.argv[1:])
  stomp = StompTransport()
  stomp.connect()

  txn = transaction = stomp.transaction_begin()
  sid = workflows.recipe.wrap_subscribe(
      stomp, results_queue, process_result,
      acknowledgement=True, exclusive=True,
      allow_non_recipe_messages=True,
  )
  wait_until_idle(3)
#  stomp.unsubscribe(sid) ### Currently workflows wrap subscriptions do not allow unsubscribing.
                          ### https://github.com/DiamondLightSource/python-workflows/issues/17
  # Stop processing any further messages
  transaction = None
  wait_until_idle(0.3)

  # Put messages back on results queue, but without recipe bulk
  # Create JUnit result records
  junit_results = []
  for test_history in test_results.values():
    relevant_test = None
    for test in test_history:
      stomp.send(results_queue, test, transaction=txn)
      if not relevant_test:
        relevant_test = test
      elif test['success'] is not None and relevant_test['success'] is None:
        relevant_test = test
      elif test['time_start'] > relevant_test['time_start']:
        relevant_test = test
    if not relevant_test:
      continue
    r = dlstbx.util.result.Result()
    r.set_name(relevant_test['scenario'])
    r.set_classname("{test[beamline]}.{test[scenario]}".format(test=relevant_test))
    r.log_message('Started at {start:%Y-%m-%d %H:%M:%S}, finished at {end:%Y-%m-%d %H:%M:%S}, took {elapsed:.1f} seconds.'.format(
        start=datetime.datetime.fromtimestamp(relevant_test['time_start']),
        end=datetime.datetime.fromtimestamp(relevant_test['time_end']),
        elapsed=relevant_test['time_end'] - relevant_test['time_start'],
    ))
    if relevant_test['success'] is None:
      r.log_skip('No results arrived yet')
    elif relevant_test['success'] is True:
      r.log_message('Test successful')
    else:
      r.log_error(relevant_test.get('reason', 'Test failed'))
    r.set_time(relevant_test['time_end'] - relevant_test['time_start'])
    junit_results.append(r)

  # Done.
  stomp.transaction_commit(txn)

  # Export results
  ts = junit_xml.TestSuite("Simulated data collections", junit_results)
  with open('output.xml', 'w') as f:
    junit_xml.TestSuite.to_file(f, [ts], prettyprint=True)

  wait_until_idle(0.3)
