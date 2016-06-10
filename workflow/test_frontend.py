import dlstbx.workflow.services.sample_service
import frontend
import mock

@mock.patch('dlstbx.workflow.frontend.multiprocessing')
@mock.patch('dlstbx.workflow.frontend.status.StatusAdvertise')
@mock.patch('dlstbx.workflow.frontend.dlstbx.workflow.transport')
def test_start_service_in_frontend(mock_transport, mock_status, mock_mp):
  mock_service = mock.Mock()
  mock_mp.Queue.return_value = mock.sentinel.Queue

  # initialize framework
  fe = frontend.Frontend()

  # check status information is being broadcast
  mock_status.assert_called_once()
  mock_status.return_value.start.assert_called_once()

  # start service
  fe.switch_service(mock_service)

  # check service was started properly
  mock_service.assert_called_once_with(commands=mock.sentinel.Queue, frontend=mock.sentinel.Queue)
  mock_mp.Process.assert_called_once_with(target=mock_service.return_value.start, args=())
  mock_mp.Process.return_value.start.assert_called_once()
