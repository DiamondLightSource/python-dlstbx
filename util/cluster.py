from __future__ import absolute_import, division
from dials.util.procrunner import run_process
import os

def get_dlscluster_environment():
  '''Extract environment variables needed for DLS cluster access using the
     module file. Cache results.'''

  self = get_dlscluster_environment
  if hasattr(self, 'cached'):
    return self.cached

  result = run_process(command=['/bin/bash', '-l'], timeout=10,
      stdin="module load global/testcluster\nset\n", print_stdout=False, print_stderr=False)
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
  self.cached = environment
  return environment

def get_dlstestcluster_environment():
  '''Extract environment variables needed for DLS cluster access using the
     module file. Cache results.'''

  self = get_dlstestcluster_environment
  if hasattr(self, 'cached'):
    return self.cached

  result = run_process(command=['/bin/bash', '-l'], timeout=10,
      stdin="module load global/testcluster\nset\n", print_stdout=False, print_stderr=False)
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
  self.cached = environment
  return environment

class Cluster():
  '''DRMAA access to DLS computing clusters'''

  # Reference to encapsulated DRMAA module and session
  drmaa = None
  session = None

  def __init__(self, name):
    '''Interface to a computing cluster
       :param name: Either 'dlscluster' or 'dlstestcluster'.
    '''

    if name == 'dlscluster':
      self.environment = get_dlscluster_environment()
    elif name == 'dlstestcluster':
      self.environment = get_dlstestcluster_environment()
    else:
      raise RuntimeError('Need to specify name of cluster to accecss (dlscluster/dlstestcluster)')

    for k, v in self.environment.iteritems():
      os.environ[k] = v
    import drmaa
    self.drmaa = drmaa
    self.session = drmaa.Session()
    self.session.initialize()
    print('A session was started successfully')

  def contact(self):
    return self.session.contact

  def qsub(self):
    job = self.session.createJobTemplate()
    job.remoteCommand = '/bin/bash'
    job.args = [ '-c', 'touch markerfile; sleep 120; ls -la' ]
    job.joinFiles = True
    jobid = self.session.runJob(job)
    print "Job submitted as %s" % jobid

    retval = self.session.wait(jobid, self.drmaa.Session.TIMEOUT_WAIT_FOREVER)
    print('Job: {0} finished with status {1}'.format(retval.jobId, retval.hasExited))

    print('Cleaning up')
    self.session.deleteJobTemplate(job)

  def close(self):
    if self.session:
      self.session.exit()
      print "Session closed."
    self.session = None
    self.drmaa = None

  def __del__(self):
    self.close()

if __name__ == '__main__':
  c = Cluster('dlstestcluster')
  print c.qsub()
