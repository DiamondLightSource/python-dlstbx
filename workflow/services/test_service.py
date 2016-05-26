from __future__ import division

import dlstbx.workflow.services
import mock
import multiprocessing
import Queue
import pytest

def test_instantiate_basic_service():
  '''Create a basic service object'''
  service = dlstbx.workflow.services.Service()

def test_logging_to_frontend():
  '''Log messages should be passed to frontend'''
  fe_queue = mock.Mock()
  service = dlstbx.workflow.services.Service(
      frontend=fe_queue)

  service.log(mock.sentinel.logmessage)

  fe_queue.put.assert_called()
  assert fe_queue.put.call_args \
      == (({ 'log': mock.sentinel.logmessage },), {})

def test_logging_to_dummy():
  '''Should run without errors, message should be dropped.'''
  service = dlstbx.workflow.services.Service()
  service.log(mock.sentinel.logmessage)

def test_receive_and_follow_shutdown_command():
  '''Receive a shutdown message via the command queue and act on it.
     Check that status codes are updated properly.'''
  cmd_queue = mock.Mock()
  cmd_queue.get.side_effect = [ 
    { 'channel': 'command', 'payload': 'shutdown' },
    AssertionError('Not observing commands') ]
  fe_queue = Queue.Queue()

  # Create service
  service = dlstbx.workflow.services.Service(
      command=cmd_queue, frontend=fe_queue)
  # override class API to ensure overidden functions are called
  service.initialize = mock.Mock()
  service.shutdown = mock.Mock()

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
  service.initialize.assert_called_once()
  service.shutdown.assert_called_once()
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
