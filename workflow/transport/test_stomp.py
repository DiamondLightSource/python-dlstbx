from __future__ import division
from dlstbx.workflow.transport.stomp import Transport
import mock

def test_initialize_stomp_transport_layer():
  Transport()

def test_add_command_line_help():
  '''Check that command line parameters are registered in the parser.'''
  parser = mock.MagicMock()

  Transport().add_command_line_options(parser)

  assert parser.add_option.called
  assert parser.add_option.call_count > 4
  for call in parser.add_option.call_args_list:
    assert call[1]['action'] == 'callback'

@mock.patch('dlstbx.workflow.transport.stomp.stomp')
def test_instantiate_link_and_connect_to_broker(mockstomp):
  '''Test the Stomp connection routine.'''
  mockconn = mock.Mock()
  mockstomp.Connection.return_value = mockconn

  stomp = Transport()
  assert not stomp.is_connected()

  stomp.connect()

  mockstomp.Connection.assert_called_once()
  mockconn.start.assert_called_once()
  mockconn.connect.assert_called_once()
  assert stomp.is_connected()

  stomp.connect()

  mockstomp.Connection.assert_called_once()
  mockconn.start.assert_called_once()
  mockconn.connect.assert_called_once()
  assert stomp.is_connected()

# optparse code & tests
