from __future__ import absolute_import, division
from dials.util.procrunner import run_process
import multiprocessing
import os
import threading
import time

_DLS_Load_Cluster = "module load global/cluster"
_DLS_Load_Testcluster = "module load global/testcluster"

class Cluster():
  '''DRMAA access to DLS computing clusters'''

  def __init__(self, clustername):
    '''Interface to a computing cluster
       :param name: Either 'dlscluster' or 'dlstestcluster'.
    '''

    if clustername == 'dlscluster':
      self.environment = self.load_environment(_DLS_Load_Cluster)
    elif clustername == 'dlstestcluster':
      self.environment = self.load_environment(_DLS_Load_Testcluster)
    else:
      raise RuntimeError('Need to specify name of cluster to accecss (dlscluster/dlstestcluster)')

    # Set up drmaa subprocess. Create two bidirectional pipes,
    # one for the main process and one for the drmaa subprocess.
    self._pipe_main, self._pipe_subprocess = multiprocessing.Pipe()
    self._subprocess = multiprocessing.Process(target=self.start, args=())
    self._subprocess.daemon = True
    self._subprocess.start()

    # At this point must close the pipe that is unused by this process
    self._pipe_subprocess.close()
    del(self._pipe_subprocess)

    self.qstat = self.create_remote_function_call('_qstat')
    self.qsub  = self.create_remote_function_call('_qsub')

    self.lock = threading.Lock()

  def create_remote_function_call(self, remote_function_name):
    def stub(*args, **kwargs):
      with self.lock:
        self._pipe_main.send((remote_function_name, args, kwargs))
        try:
          retval = self._pipe_main.recv()
        except EOFError:
          self.close()
          raise RuntimeError("Subprocess died")
      if 'value' in retval:
        return retval['value']
      if 'exception' in retval:
        e = retval['exception']
        if hasattr(e, 'trace'):
          # If exception contains extended information, encode this into the exception message,
          # preserving the exception type:
          e.args = ("%s. Embedded exception follows:\n%s" % (str(e), e.trace),)
        raise e # re-raise subprocess exception
    return stub

  def start(self):
    '''Main function of the drmaa subprocess.
       Start a session and wait for remote procedure calls from main thread.
    '''

    # First close the other pipe unused by this process
    self._pipe_main.close()
    del(self._pipe_main)

    # Load DRMAA
    for k, v in self.environment.iteritems():
      os.environ[k] = v
    import drmaa
    self.drmaa = drmaa
    self.session = drmaa.Session()
    self.session.initialize()

    # Wait and process RPCs
    while True:
      try:
        function, args, kwargs = self._pipe_subprocess.recv()
      except EOFError:
        break
      try:
        retval = getattr(self, function)(*args, **kwargs)
        self._pipe_subprocess.send({'value': retval})
      except Exception, e:
        import sys, traceback
        # Keep a formatted copy of the trace for passing in serialized form
        trace = [ "  %s" % line for line in traceback.format_exception(*sys.exc_info()) ]
        e.trace = "\n" + "\n".join(trace)
        self._pipe_subprocess.send({'exception': e})
    self._pipe_subprocess.close()
    del(self._pipe_subprocess)

  @classmethod
  def load_environment(cls, command):
    '''Extract environment variables needed for cluster access from a
       command line. Cache the results.'''

    if not hasattr(cls, 'cached_environment'):
      cls.cached_environment = {}
    if command in cls.cached_environment:
      return cls.cached_environment[command]

    result = run_process(command=['/bin/bash', '-l'], timeout=10,
        stdin=command + "\nset\n", print_stdout=False, print_stderr=False)
    if result['timeout'] or result['exitcode'] != 0:
      print result
      raise RuntimeError('Could not load cluster environment\n%s' % str(result))

    environment = {}
    for line in result['stdout'].split('\n'):
      if '=' in line:
        line = line.strip()
        variable, content = line.split('=', 1)
        if variable.startswith(('DRMAA_', 'SGE_')):
          environment[variable] = content
    cls.cached_environment[command] = environment
    return environment

  def close(self):
    if hasattr(self, '_pipe_main'):
      self._pipe_main.close()
      del(self._pipe_main)
    if self._subprocess:
      self._subprocess.join()
      self._subprocess = None

  def _qstat(self, jobid):
    if isinstance(jobid, int):
      jobid = str(jobid)
    # Who needs a case statement when you have dictionaries?
    decodestatus = {self.drmaa.JobState.UNDETERMINED: 'process status cannot be determined',
                        self.drmaa.JobState.QUEUED_ACTIVE: 'job is queued and active',
                        self.drmaa.JobState.SYSTEM_ON_HOLD: 'job is queued and in system hold',
                        self.drmaa.JobState.USER_ON_HOLD: 'job is queued and in user hold',
                        self.drmaa.JobState.USER_SYSTEM_ON_HOLD: 'job is queued and in user and system hold',
                        self.drmaa.JobState.RUNNING: 'job is running',
                        self.drmaa.JobState.SYSTEM_SUSPENDED: 'job is system suspended',
                        self.drmaa.JobState.USER_SUSPENDED: 'job is user suspended',
                        self.drmaa.JobState.DONE: 'job finished normally',
                        self.drmaa.JobState.FAILED: 'job finished, but failed'}
    return decodestatus[self.session.jobStatus(jobid)]

  def _qsub(self, asdf):
    job = self.session.createJobTemplate()
    job.remoteCommand = '/bin/bash'
    import uuid
    job.args = [ '-c', 'touch markerfile.' + str(uuid.uuid4()) + '; sleep 120; ls -la' ]
    job.joinFiles = True
    jobid = self.session.runJob(job)
    print "Job submitted as %s" % jobid
    self.session.deleteJobTemplate(job)
    return(jobid)

    retval = self.session.wait(jobid, self.drmaa.Session.TIMEOUT_WAIT_FOREVER)
    print('Job: {0} finished with status {1}'.format(retval.jobId, retval.hasExited))

if __name__ == '__main__':
  rc = Cluster('dlscluster')
  tc = Cluster('dlstestcluster')
  for x in xrange(3):
    print "Submitted:", tc.qsub('asdf')
    print "Submitted:", rc.qsub('asdf')
    print "Cluster",     rc.qstat(17048040)
    print "Testcluster", tc.qstat(1468)
    time.sleep(0.5)
  tc.close()
  rc.close()

