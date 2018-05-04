from __future__ import absolute_import, division, print_function

import logging
import multiprocessing
import os
import threading
import time
from datetime import datetime

from procrunner import run_process

_DLS_Load_Cluster = ". /etc/profile.d/modules.sh ; module load global/cluster"
_DLS_Load_Testcluster = ". /etc/profile.d/modules.sh ; module load global/testcluster"

log = logging.getLogger('dlstbx.util.cluster')

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
      raise RuntimeError('Need to specify name of cluster to access (dlscluster/dlstestcluster)')

    # Set up drmaa subprocess. Create two bidirectional pipes,
    # one for the main process and one for the drmaa subprocess.
    self._pipe_main, self._pipe_subprocess = multiprocessing.Pipe()
    self._subprocess = multiprocessing.Process(target=self.start, args=())
    self._subprocess.name = 'dlstbx.util.cluster for ' + clustername
    self._subprocess.daemon = True
    self._subprocess.start()

    # At this point must close the pipe that is unused by this process
    self._pipe_subprocess.close()
    del(self._pipe_subprocess)

    self.qstat = self.create_remote_function_call('_qstat')
    self.qstat_xml = self.create_remote_function_call('_qstat_xml')
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
      except Exception as e:
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

    blank_environment = { k: '' for k in
                          filter(lambda k: k.startswith('DRMAA_') or k.startswith('SGE_'),
                                 os.environ) }

    result = run_process(command=['/bin/bash', '-l'], timeout=10,
        stdin=command + "\nset\n", print_stdout=False, print_stderr=False, environment_override=blank_environment)
    if result['timeout'] or result['exitcode'] != 0:
      raise RuntimeError('Could not load cluster environment\n%s' % str(result))

    environment = {}
    for line in result['stdout'].split('\n'):
      if '=' in line:
        line = line.strip()
        variable, content = line.split('=', 1)
        if variable.startswith(('DRMAA_', 'SGE_', 'PATH')):
          environment[variable] = content
    for variable in ('DRMAA_LIBRARY_PATH', 'SGE_ROOT', 'PATH'):
      if not environment.get(variable):
        raise RuntimeError('Could not load cluster environment, required variable %s unset' % variable)
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
    '''Get the status of a single job with known ID.'''
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

  def _qstat_xml(self, arguments=None, timeout=45, warn_after=20):
    '''Run a qstat command against the cluster
       :param arguments: List of command line parameters
       :param timeout: maximum execution time
       :return: A result dictionary, containing stdout, stderr, exitcode, and more.
    '''
    if not arguments: arguments = []
    result = run_process(command=['qstat', '-xml'] + arguments, timeout=timeout,
        stdin='', print_stdout=False, print_stderr=False)
    if result['timeout']:
      log.error('failed to read cluster statistics after %.1f seconds', result['runtime'])
    elif result['runtime'] > warn_after:
      log.warn('reading cluster statistics took %.1f seconds', result['runtime'])
    else:
      log.debug('reading cluster statistics took %.1f seconds', result['runtime'])
    return result

  def _qsub(self, command, arguments, job_params=None):
    '''Submit a job to the cluster
       :param command: String pointing to an executable.
                       This must be reachable, ie. not be on a local drive.
       :param arguments: List of strings, command line arguments.
       :return: A string containing the job ID of the submission
    '''
    job = self.session.createJobTemplate()
    job.remoteCommand = command
    job.args = arguments
    job.joinFiles = True
    native_spec = []
    if job_params:
      native_spec.append('-pe smp 1')
      if job_params.get('now'):
        native_spec += ['-now', 'yes']
      if job_params.get('time_limit'):
        native_spec += ['-l', 'h_rt=' + job_params.get('time_limit')]
      if job_params.get('nproc'):
        native_spec += ['-pe', 'smp', str(job_params.get('nproc'))]
      if job_params.get('queue'):
        native_spec += ['-q', job_params.get('queue')]
    if native_spec:
      job.nativeSpecification = ' '.join(native_spec)
    jobid = self.session.runJob(job)
    self.session.deleteJobTemplate(job)
    return(jobid)

  def _qwait(self, jobid):
    retval = self.session.wait(jobid, self.drmaa.Session.TIMEOUT_WAIT_FOREVER)
    print('Job: {0} finished with status {1}'.format(retval.jobId, retval.hasExited))


class ClusterStatistics():
  '''Interface to qstat'''

  @staticmethod
  def convert_string_to_time(s):
    return datetime.strptime(s + '000', '%Y-%m-%dT%H:%M:%S.%f')

  def age(self, dt):
    if not hasattr(self, '_age_now'):
      setattr(self, '_age_now', datetime.now())
    return (getattr(self, '_age_now') - dt).total_seconds()

  def parse_job_xml(self, j):
    '''Parse qstat -xml job information into a dictionary'''
    job = {}
    job['state'] = j.attributes['state'].value
    job['slots'] = int(j.getElementsByTagName('slots')[0].firstChild.nodeValue)
    job['ID'] = int(j.getElementsByTagName('JB_job_number')[0].firstChild.nodeValue)
    job['name'] = j.getElementsByTagName('JB_name')[0].firstChild.nodeValue
    job['owner'] = j.getElementsByTagName('JB_owner')[0].firstChild.nodeValue
    job['statecode'] = j.getElementsByTagName('state')[0].firstChild.nodeValue
    if job['state'] == 'running':
      job['start'] = self.convert_string_to_time(j.getElementsByTagName('JAT_start_time')[0].firstChild.nodeValue)
      job['age'] = self.age(job['start'])
      job['queue'], job['node'] = (j.getElementsByTagName('queue_name') or j.parentNode.getElementsByTagName('name'))[0].firstChild.nodeValue.split('@')
    if job['state'] == 'pending':
      job['submission'] = self.convert_string_to_time(j.getElementsByTagName('JB_submission_time')[0].firstChild.nodeValue)
      job['age'] = self.age(job['submission'])
      job['queue'] = j.getElementsByTagName('hard_req_queue')
      if job['queue']:
        job['queue'] = job['queue'][0].firstChild.nodeValue
      else:
        job['queue'] = ''
    return job

  @staticmethod
  def parse_queue_xml(q):
    '''Parse qstat -xml queue information into a dictionary'''
    queue = {}
    queue['ID'] = q.getElementsByTagName('name')[0].firstChild.nodeValue
    queue['class'], queue['host'] = queue['ID'].split('@')
    queue['slots_used'] = int(q.getElementsByTagName('slots_used')[0].firstChild.nodeValue)
    queue['slots_reserved'] = int(q.getElementsByTagName('slots_resv')[0].firstChild.nodeValue)
    queue['slots_total'] = int(q.getElementsByTagName('slots_total')[0].firstChild.nodeValue)
    queue['slots_free'] = queue['slots_total'] - queue['slots_used'] - queue['slots_reserved']
    queue['state'] = q.getElementsByTagName('state')
    if queue['state']:
      queue['state'] = queue['state'][0].firstChild.nodeValue
    else:
      queue['state'] = ''
    queue['disabled'] = any(char in queue['state'] for char in 'odD')
    queue['error'] = not queue['disabled'] and 'E' in queue['state']
    queue['enabled'] = not queue['disabled'] and not queue['error']
    queue['unknown'] = 'u' in queue['state']
    queue['alarm'] = any(char in queue['state'] for char in 'aA') and queue['enabled']
    if q.getElementsByTagName('load-alarm-reason'):
      queue['alarm-reason'] = q.getElementsByTagName('load-alarm-reason')[0].firstChild.nodeValue
    if q.getElementsByTagName('message'):
      queue['message'] = q.getElementsByTagName('message')[0].firstChild.nodeValue
    queue['suspended'] = any(char in queue['state'] for char in 'sSC') or queue['unknown'] or not queue['enabled']
    if queue['suspended'] or queue['error']:
      queue['slots_free'] = 0
    return queue

  def parse_string(self, string):
    '''Parse a string containing the XML output of qstat and
       return a list of job and a list of queue dictionaries.'''
    import xml.dom.minidom
    return self.parse_xml(xml.dom.minidom.parseString(string))

  def parse_xml(self, xmldom):
    '''Given an XML object return a list of job and a list of queue dictionaries.'''
    joblist = xmldom.getElementsByTagName('job_list')
    joblist = map(self.parse_job_xml, joblist)
    queuelist = xmldom.getElementsByTagName('Queue-List')
    queuelist = map(self.parse_queue_xml, queuelist)
    return (joblist, queuelist)

  @staticmethod
  def get_nodelist_from_queuelist(queuelist):
    cluster_nodes = {}
    for q in queuelist:
      try:
        cluster_nodes[q['host']].append(q)
      except KeyError:
        cluster_nodes[q['host']] = [q]
    return cluster_nodes

  @staticmethod
  def summarize_node_status(node):
    summary = { 'status': 'broken', 'running_queues': 0 }
    for queue in node:
      summary[queue['class']] = { 'slots': queue['slots_total'], 'used': queue['slots_used'], 'reserved': queue['slots_reserved'] }
      if queue['error']:
        summary[queue['class']]['status'] = 'broken'
      elif queue['suspended']:
        summary[queue['class']]['status'] = 'suspended'
      elif queue['disabled'] or not queue['enabled']:
        summary[queue['class']]['status'] = 'broken'
      else:
        summary[queue['class']]['status'] = 'running'
        summary['running_queues'] += 1
    if summary['running_queues']:
      summary['status'] = 'running'
    return summary

  def run_on(self, cluster, arguments=None):
    '''Run qstat on cluster object and return parsed output.

       :return a list of job and a list of queue dictionaries otherwise.
    '''
    result = cluster.qstat_xml(arguments=arguments)
    assert not result['timeout'] and result['exitcode'] == 0, 'Could not run qstat on cluster. timeout: %s, exitcode: %s, Output: %s' % \
                                                              (str(result.get('timeout')), str(result.get('exitcode')), result.get('stderr', '') or result.get('stdout', ''))
    return self.parse_string(result['stdout'])


if __name__ == '__main__':
  rc = Cluster('dlscluster')
  tc = Cluster('dlstestcluster')
  stats = ClusterStatistics()
  import uuid
  from pprint import pprint
  test_id = tc.qsub('/bin/bash', [ '-c', 'touch markerfile.' + str(uuid.uuid4()) + '; sleep 10; ls -la' ])
  real_id = rc.qsub('/bin/bash', [ '-c', 'touch markerfile.' + str(uuid.uuid4()) + '; sleep 10; ls -la' ])
  print("Submitted job #%s to the cluster and #%s to the testcluster" % (real_id, test_id))

  joblist, _ = stats.run_on(rc, arguments=['-r', '-u', '*'])
  joblist = filter(lambda j: j['ID'] == int(real_id), joblist)
  if len(joblist) < 1:
    print("Could not read back information about this job ID from cluster")
  else:
    if len(joblist) > 1:
      print("Found more than one job with this ID on cluster")
      pprint(joblist)
    else:
      print("Job information on cluster:")
      pprint(joblist[0])

  joblist, _ = stats.run_on(tc, arguments=['-r', '-u', '*'])
  joblist = filter(lambda j: j['ID'] == int(test_id), joblist)
  if len(joblist) < 1:
    print("Could not read back information about this job ID from testcluster")
  else:
    if len(joblist) > 1:
      print("Found more than one job with this ID on testcluster")
      pprint(joblist)
    else:
      print("Job information on testcluster:")
      pprint(joblist[0])

  for x in xrange(4):
    time.sleep(4)
    print("Cluster",     rc.qstat(real_id))
    print("Testcluster", tc.qstat(test_id))
  tc.close()
  rc.close()
