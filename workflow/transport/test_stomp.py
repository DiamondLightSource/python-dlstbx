from __future__ import division
import dlstbx.workflow.transport
from dlstbx.workflow.transport.stomp import Transport
import mock
import optparse
import os

def test_lookup_and_initialize_stomp_transport_layer():
  '''Find the stomp transport layer via the lookup mechanism and run
     its constructor with default settings.'''
  stomp = dlstbx.workflow.transport.lookup("stomp")
  assert stomp == Transport
  stomp()

def test_add_command_line_help():
  '''Check that command line parameters are registered in the parser.'''
  parser = mock.MagicMock()

  Transport().add_command_line_options(parser)

  assert parser.add_option.called
  assert parser.add_option.call_count > 4
  for call in parser.add_option.call_args_list:
    assert call[1]['action'] == 'callback'

@mock.patch('dlstbx.workflow.transport.stomp.stomp')
def test_check_config_file_behaviour(mockstomp):
  '''Check that a specified configuration file is read, that command line
     parameters have precedence and are passed on to the stomp layer.'''
  cfgfile = os.path.join(os.path.dirname(os.path.realpath(__file__)), \
                         'test_stomp.cfg')
  mockconn = mock.Mock()
  mockstomp.Connection.return_value = mockconn
  parser = optparse.OptionParser()
  stomp = Transport()
  stomp.add_command_line_options(parser)

  parser.parse_args([
    '--stomp-conf', cfgfile,
    '--stomp-user', mock.sentinel.user])

  stomp.connect()

  mockstomp.Connection.assert_called_once_with([('localhost', 1234)])
  mockconn.connect.assert_called_once_with(mock.sentinel.user, 'somesecret', wait=True)
  assert stomp.get_namespace() == 'namespace'

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
