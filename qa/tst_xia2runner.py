import mock
import unittest
import xia2runner

class Xia2runnerTests(unittest.TestCase):

  @unittest.skipIf(xia2runner._dummy, 'xia2runner class set to dummy mode')
  @mock.patch('xia2runner._NonBlockingStreamReader')
  @mock.patch('xia2runner.time')
  @mock.patch('xia2runner.subprocess')
  def test_run_command_aborts_after_timeout(self, mock_subprocess, mock_time, mock_streamreader):
    mock_process = mock.Mock()
    mock_process.returncode = None
    mock_subprocess.Popen.return_value = mock_process
    task = ['___']

    with self.assertRaises(Exception):
      xia2runner._run_with_timeout(task, -1, False)

    self.assertTrue(mock_subprocess.Popen.called)
    self.assertTrue(mock_process.terminate.called)
    self.assertTrue(mock_process.kill.called)


  @unittest.skipIf(xia2runner._dummy, 'xia2runner class set to dummy mode')
  @mock.patch('xia2runner._NonBlockingStreamReader')
  @mock.patch('xia2runner.subprocess')
  def test_run_command_runs_command_and_directs_pipelines(self, mock_subprocess, mock_streamreader):
    (mock_stdout, mock_stderr) = (mock.Mock(), mock.Mock())
    mock_stdout.get_output.return_value = mock.sentinel.proc_stdout
    mock_stderr.get_output.return_value = mock.sentinel.proc_stderr
    (stream_stdout, stream_stderr) = (mock.sentinel.stdout, mock.sentinel.stderr)
    mock_process = mock.Mock()
    mock_process.stdout = stream_stdout
    mock_process.stderr = stream_stderr
    mock_process.returncode = 99
    command = ['___']
    def streamreader_processing(*args):
      return {(stream_stdout,): mock_stdout, (stream_stderr,): mock_stderr}[args]
    mock_streamreader.side_effect = streamreader_processing
    mock_subprocess.Popen.return_value = mock_process

    expected = {
      'stderr': mock.sentinel.proc_stderr,
      'stdout': mock.sentinel.proc_stdout,
      'exitcode': mock_process.returncode,
      'command': ' '.join(command),
      'runtime': mock.ANY,
      'timeout': False,
    }

    actual = xia2runner._run_with_timeout(command, 0.5, False)

    self.assertTrue(mock_subprocess.Popen.called)
    mock_streamreader.assert_has_calls([mock.call(stream_stdout,), mock.call(stream_stderr,)], any_order=True)
    self.assertFalse(mock_process.terminate.called)
    self.assertFalse(mock_process.kill.called)
    self.assertEquals(actual, expected)


  @unittest.skip('Not implemented yet')
  def test_nonblockingstreamreader_tbd(self):
    command = ['-parameter']
    workdir = 'workdir'
    datadir = 'datadir'
    archivejson = 'archive.json'
    timeout = 60

    #os.unlink
    #os.makedirs
    #os.chdir

    xia2runner.runxia2(command, workdir, datadir, archivejson, timeout)

#  @unittest.skip('Not implemented yet')
#  def test_runxia2_tbd(self):

#  @unittest.skip('Not implemented yet')
#  def test_compress_file_tbd(self):


if __name__ == '__main__':
  unittest.main()
