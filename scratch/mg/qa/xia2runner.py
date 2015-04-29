import testsuite

_debug = True
_dummy = False

class _NonBlockingStreamReader:
  '''Reads a stream in a thread to avoid blocking/deadlocks'''
  def __init__(self, stream, output=True):
    import cStringIO as StringIO
    from threading import Thread

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


def _run_with_timeout(command, timeout):
  import cStringIO as StringIO
  import time
  import timeit
  import subprocess
  import sys

  if _debug:
    print "Starting external process:", command
  start_time = timeit.default_timer()
  max_time = start_time + timeout
  p = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  stdout = _NonBlockingStreamReader(p.stdout)
  stderr = _NonBlockingStreamReader(p.stderr)

  block = 4096 # block size to read from pipes
  timeout = False
  
  while (timeit.default_timer() < max_time) and (p.returncode is None):
    if _debug:
      print "still running (T%.2fs)" % (timeit.default_timer() - max_time)

    # sleep some time
    try:
      time.sleep(0.5)
    except KeyboardInterrupt:
      p.kill() # if user pressed Ctrl+C we won't be able to produce a proper report anyway
               # but at least should make sure the process dies with us
      sys.exit(1)

    # check if process is still running
    p.poll()

  if p.returncode is None:
    # timeout condition
    timeout = True
    if _debug:
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
  if _debug:
    print "Process ended after %.1f seconds with exit code %d (T%.2fs)" % \
      (runtime, p.returncode, timeit.default_timer() - max_time)

  stdout = stdout.get_output()
  stderr = stderr.get_output()

  result = { 'exitcode': p.returncode,
             'stdout': stdout, 'stderr': stderr,
             'timeout': timeout, 'runtime': runtime }
  return result


def xia2(*args, **kwargs):
  import os
  import shutil

  module = testsuite.getModule()
  workdir = os.path.join(module['workdir'], module['currentTest'][0])
  datadir = module['datadir']
  archivedir = os.path.join(module['archivedir'], module['currentTest'][0])
  timeout = 3600
  if 'timeout' in module['currentTest'][3]:
    timeout = module['currentTest'][3]['timeout']

  print "=========="
  print "running test ", args, kwargs
  print "Workdir:", workdir
  print "Datadir:", datadir
  print "Decoration:", module['currentTest'][3]
  print "Timeout:", timeout
  print "Archive:", archivedir
  print "=========="

  # Go to working directory
  if not os.path.isdir(workdir):
    os.makedirs(workdir)
  os.chdir(workdir)

  # clear working directory
  for f in os.listdir(workdir):
    fp = os.path.join(workdir, f)
    if os.path.isfile(fp):
      if _debug:
        print "unlink", fp
      os.unlink(fp)
    elif os.path.isdir(fp):
      if _debug:
        print "rmtree", fp
      shutil.rmtree(fp)


  command = ['xia2', '-quick']
  command.extend(args)
  command.append(datadir)

  if _dummy:
    run = "dummy"
  else:
    run = _run_with_timeout(command, timeout=timeout)

  if _debug:
    print run

  result = { "resolution.low": 5, "resolution.high": 20 }

  testsuite.storeTestResults(result)
