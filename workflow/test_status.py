from __future__ import division
import status
import mock

@mock.patch('dlstbx.workflow.status.threading')
def test_status_advertiser_starts_a_new_thread(mock_threading):
  s = status.StatusAdvertise()
  mock_threading.Thread.assert_called_once()
  s.start()
  mock_threading.Thread.return_value.start.assert_called_once()

@mock.patch('dlstbx.workflow.status.threading')
@mock.patch('dlstbx.workflow.status.time')
def test_status_advertiser_regularly_passes_status(mock_time, mock_threading):
  sm = mock.Mock() # status mock
  tm = mock.Mock() # transport mock
  s = status.StatusAdvertise(interval=120, status_callback=sm, transport=tm)
  t = mock_threading.Thread.call_args[1]['target'] # target function
  mock_time.time.return_value = 100
  mock_time.sleep.side_effect = RuntimeError(mock.sentinel.pause)
  sm.return_value = mock.sentinel.status1

  # Run with a failing status function
  sm.side_effect = RuntimeError(mock.sentinel.status_error)
  try:
    t()
    assert False, "Expected exception not raised"
  except RuntimeError, e:
    assert e.message == mock.sentinel.pause

  mock_time.sleep.assert_called_once_with(120)
  sm.assert_called_once()

  # Run with a working status function
  sm.side_effect = None
  try:
    t()
    assert False, "Expected exception not raised"
  except RuntimeError, e:
    assert e.message == mock.sentinel.pause

  assert mock_time.sleep.call_count == 2
  tm.broadcast_status.assert_called_once_with(mock.sentinel.status1)

  # Run after being stopped
  s.stop()
  t() # this must no longer throw an exception

  assert sm.call_count == 2
