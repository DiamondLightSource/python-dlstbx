from __future__ import division

import dlstbx.workflow.services
import mock
import Queue

def test_instantiate_basic_service():
  '''Create a basic service object'''
  service = dlstbx.workflow.services.Service()

  assert service.get_name() is not None

def test_logging_to_frontend():
  '''Log messages should be passed to frontend'''
  fe_queue = mock.Mock()
  service = dlstbx.workflow.services.Service(frontend=fe_queue)

  service.log(mock.sentinel.logmessage)

  fe_queue.put.assert_called()
  assert fe_queue.put.call_args \
      == (({ 'log': mock.sentinel.logmessage, 'source': 'other' },), {})

def test_logging_to_dummy():
  '''Should run without errors, message should be dropped.'''
  service = dlstbx.workflow.services.Service()
  service.log(mock.sentinel.logmessage)

def test_send_status_updates_to_frontend():
  '''Status updates should be passed to frontend'''
  fe_queue = mock.Mock()
  service = dlstbx.workflow.services.Service(frontend=fe_queue)

  service.update_status(mock.sentinel.status)

  fe_queue.put.assert_called()
  assert fe_queue.put.call_args \
      == (({ 'status': mock.sentinel.status },), {})

def test_send_status_dummy():
  '''Should run without errors, status should be dropped.'''
  service = dlstbx.workflow.services.Service()
  service.update_status(mock.sentinel.status)

def test_receive_and_follow_shutdown_command():
  '''Receive a shutdown message via the command queue and act on it.
     Check that status codes are updated properly.'''
  cmd_queue = mock.Mock()
  cmd_queue.get.side_effect = [
    { 'channel': 'command',
      'payload': dlstbx.workflow.services.Commands.SHUTDOWN },
    AssertionError('Not observing commands') ]
  fe_queue = Queue.Queue()

  # Create service
  service = dlstbx.workflow.services.Service(
      commands=cmd_queue, frontend=fe_queue)
  # override class API to ensure overidden functions are called
  service.initializing = mock.Mock()
  service.in_shutdown = mock.Mock()

  # Check new status
  messages = []
  while not fe_queue.empty():
    message = fe_queue.get_nowait()
    if 'statuscode' in message:
      messages.append(message['statuscode'])
  assert messages == [ service.SERVICE_STATUS_NEW ]

  # Start service
  service.start()

  # Check startup/shutdown sequence
  service.initializing.assert_called_once()
  service.in_shutdown.assert_called_once()
  cmd_queue.get.assert_called_once()
  messages = []
  while not fe_queue.empty():
    message = fe_queue.get_nowait()
    if 'statuscode' in message:
      messages.append(message['statuscode'])
  assert messages == [
    service.SERVICE_STATUS_STARTING,
    service.SERVICE_STATUS_IDLE,
    service.SERVICE_STATUS_PROCESSING,
    service.SERVICE_STATUS_SHUTDOWN,
    service.SERVICE_STATUS_END,
    ]

def test_trigger_idle_timer():
  '''Should run without errors, message should be dropped.'''
  pass # TODO

def test_callbacks_are_routed_correctly():
  '''Incoming messages are routed to the correct callback functions'''
  pass # TODO

def test_log_unknown_channel_data():
  '''All unidentified messages should be logged to the frondend.'''
  cmd_queue = mock.Mock()
  cmd_queue.get.side_effect = [
    { 'channel': mock.sentinel.channel, 'payload': mock.sentinel.failure1 },
    { 'payload': mock.sentinel.failure2 },
    { 'channel': 'command',
      'payload': dlstbx.workflow.services.Commands.SHUTDOWN },
    AssertionError('Not observing commands') ]
  fe_queue = Queue.Queue()

  # Create service
  service = dlstbx.workflow.services.Service(
      commands=cmd_queue, frontend=fe_queue)

  # Start service
  service.start()

  # Check startup/shutdown sequence
  messages = []
  while not fe_queue.empty():
    message = fe_queue.get_nowait()
    if 'log' in message:
      messages.append(message)
  assert len(messages) == 2
  assert messages[0]['source'] == 'service' and \
         messages[0]['channel'] == mock.sentinel.channel
  assert messages[1]['source'] == 'service' and \
         messages[1].get('channel') == None

