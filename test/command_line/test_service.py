import dlstbx.command_line.service
import mock
import pytest

@mock.patch('dlstbx.command_line.service.dlstbx.workflow.frontend')
def test_instantiate_new_frontend(mock_frontend):
  '''Test creation of a new frontend object when run without parameters.'''
  dlstbx.command_line.service.run([])
  mock_frontend.Frontend.assert_called()

def test_help_output(capsys):
  '''Check that help strings are provided. (Strictly speaking this only
     validates that the program terminates when started with --help.)'''
  with pytest.raises(SystemExit):
    dlstbx.command_line.service.run(['--help'])
  out, err = capsys.readouterr()
  assert err == ''
  assert '--help' in out
