import dlstbx.workflow.services.sample_service
import frontend
import mock

@mock.patch('dlstbx.workflow.frontend.dlstbx.workflow.transport')
def test_instantiate_new_frontend(mock_transport):
  '''Create a new frontend.'''
  frontend.Frontend()

@mock.patch('dlstbx.workflow.frontend.dlstbx.workflow.transport')
def test_start_service_in_frontend(mock_transport):
  fe = frontend.Frontend()
  fe.switch_service(dlstbx.workflow.services.sample_service.Waiter)
