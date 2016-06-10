from __future__ import division

import dlstbx.workflow.services
import dlstbx.workflow.services.sample_service
import mock
import Queue

def test_service_can_be_looked_up():
  '''Attempt to look up the service by its name'''
  service_class = dlstbx.workflow.services.lookup('waiter')
  assert service_class == dlstbx.workflow.services.sample_service.Waiter
  service_class = dlstbx.workflow.services.lookup('waiter_alt')
  assert service_class == dlstbx.workflow.services.sample_service.Waiter

@mock.patch('dlstbx.workflow.services.sample_service.time')
def test_start_and_shutdown_sample_service(mock_time):
  '''Start the waiter service, process some stuff and shut it down again.'''
  cmd_queue = mock.Mock()
  cmd_queue.get.side_effect = [
    { 'channel': 'stuff', 'payload': mock.sentinel.stuff },
    { 'channel': 'command', 'payload': 'shutdown' },
    AssertionError('Not observing commands') ]
  fe_queue = Queue.Queue()

  # Create service
  service = dlstbx.workflow.services.sample_service.Waiter(
      commands=cmd_queue, frontend=fe_queue)

  # Start service
  service.start()

  # Check all messages consumed
  assert cmd_queue.get.call_count == 2

  # Check outgoing messages
  messages, logs = [], []
  while not fe_queue.empty():
    message = fe_queue.get_nowait()
    if 'statuscode' in message:
      messages.append(message['statuscode'])
    else:
      logs.append(message)
  assert messages == [
      service.SERVICE_STATUS_NEW,
      service.SERVICE_STATUS_STARTING,
      service.SERVICE_STATUS_IDLE,
      service.SERVICE_STATUS_PROCESSING,
      service.SERVICE_STATUS_IDLE,
      service.SERVICE_STATUS_PROCESSING,
      service.SERVICE_STATUS_SHUTDOWN,
      service.SERVICE_STATUS_END,
    ]
  assert len(logs) == 4
  assert mock_time.sleep.call_count == 3
