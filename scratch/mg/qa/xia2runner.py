import cStringIO as StringIO
import os
import shutil
import subprocess
import time
import timeit
from threading import Thread

_dummy = True

class _NonBlockingStreamReader:
  '''Reads a stream in a thread to avoid blocking/deadlocks'''
  def __init__(self, stream, output=True):
    self._stream = stream
    self._buffer = StringIO.StringIO()
    self._terminated = False
    self._closed = False

    def _thread_write_stream_to_buffer():
      line = True
      while line:
        line = self._stream.readline()
        if line:
          self._buffer.write(line)
          if output:
            print line,
      self._terminated = True

    self._thread = Thread(target = _thread_write_stream_to_buffer)
    self._thread.daemon = True
    self._thread.start()

  def has_finished(self):
    return self._terminated

  def get_output(self):
    if not self.has_finished():
      raise Exception('thread did not terminate')
    if self._closed:
      raise Exception('streamreader double-closed')
    self._closed = True
    data = self._buffer.getvalue()
    self._buffer.close()
    return data


def _run_with_timeout(command, timeout, debug):
  if debug:
    print "Starting external process:", command
  if _dummy:
    return { 'exitcode': 0, 'command': " ".join(command),
             'stdout': '', 'stderr': '',
             'timeout': False, 'runtime': 0 }
  start_time = timeit.default_timer()
  max_time = start_time + timeout
  p = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  stdout = _NonBlockingStreamReader(p.stdout)
  stderr = _NonBlockingStreamReader(p.stderr)

  block = 4096 # block size to read from pipes
  timeout = False

  while (timeit.default_timer() < max_time) and (p.returncode is None):
    if debug:
      print "still running (T%.2fs)" % (timeit.default_timer() - max_time)

    # sleep some time
    try:
      time.sleep(0.5)
    except KeyboardInterrupt:
      p.kill() # if user pressed Ctrl+C we won't be able to produce a proper report anyway
      raise    # but at least should make sure the process dies with us

    # check if process is still running
    p.poll()

  if p.returncode is None:
    # timeout condition
    timeout = True
    if debug:
      print "timeout (T%.2fs)" % (timeit.default_timer() - max_time)

    # send terminate signal and wait some time for buffers to be read
    p.terminate()
    time.sleep(0.5)
    if (not stdout.has_finished() or not stderr.has_finished()):
      time.sleep(2)
    p.poll()

  if p.returncode is None:
    # thread still alive
    # send kill signal and wait some more time for buffers to be read
    p.kill()
    time.sleep(0.5)
    if (not stdout.has_finished() or not stderr.has_finished()):
      time.sleep(5)
    p.poll()

  if p.returncode is None:
    raise Exception("Process won't terminate")

  runtime = timeit.default_timer() - start_time
  if debug:
    print "Process ended after %.1f seconds with exit code %d (T%.2fs)" % \
      (runtime, p.returncode, timeit.default_timer() - max_time)

  stdout = stdout.get_output()
  stderr = stderr.get_output()

  result = { 'exitcode': p.returncode, 'command': " ".join(command),
             'stdout': stdout, 'stderr': stderr,
             'timeout': timeout, 'runtime': runtime }
  return result


def runxia2(command, workdir, timeout, debug=1):
  if debug:
    print "=========="
    print "running test ", command
    print "Workdir:", workdir
    print "Timeout:", timeout
    print "=========="

  # Go to working directory
  if not os.path.isdir(workdir):
    os.makedirs(workdir)
  os.chdir(workdir)

  # clear working directory
  if not _dummy:
    for f in os.listdir(workdir):
      fp = os.path.join(workdir, f)
      if os.path.isfile(fp):
        if debug:
          print "unlink", fp
        os.unlink(fp)
      elif os.path.isdir(fp):
        if debug:
          print "rmtree", fp
        shutil.rmtree(fp)

  runcmd = ['xia2', '-quick']
  runcmd.extend(command)

  run = _run_with_timeout(runcmd, timeout=timeout, debug=(debug>=2))
  if debug:
    print run

  jsonfile = os.path.join(workdir, 'xia2.json')
  if os.path.isfile(jsonfile):
    run['jsonfile'] = jsonfile

  success = (run['exitcode'] == 0) and (run['timeout'] == False) \
     and os.path.isfile(jsonfile) \
     and not os.path.isfile(os.path.join(workdir, 'xia2.error'))

  run['success'] = success
  return run

def compress_file(filename, timeout=300, debug=True):
  xz = _run_with_timeout(['xz', '-9ef', filename], timeout=timeout, debug=debug)
  xz['success'] = not xz['timeout'] and (xz['exitcode'] == 0)
  return xz
