from __future__ import absolute_import, division
from collections import Counter
from dials.util.procrunner import run_process
from dlstbx.util.cluster import ClusterStatistics
import errno
import logging
import os
import random
import string
import workflows.recipe
from workflows.services.common_service import CommonService

class DLSCluster(CommonService):
  '''A service to interface zocalo with cluster functions. Here we can start
     new jobs and gather cluster statistics.'''

  # Human readable service name
  _service_name = "DLS Cluster service"

  # Logger name
  _logger_name = 'dlstbx.services.cluster'

  def initializing(self):
    '''Subscribe to the cluster submission queue.
       Received messages must be acknowledged.'''
    self.log.info("Cluster service starting")

    workflows.recipe.wrap_subscribe(
      self._transport,
      'cluster.submission',
      self.run_submit_job,
      acknowledgement=True)

    # Generate cluster statistics up to every 30 seconds.
    # Statistics go with debug level to a separate logger so they can be
    # filtered by log monitors.
    self.stats_log = logging.getLogger(self._logger_name + '.stats')
    self.stats_log.setLevel(logging.DEBUG)
    self._register_idle(30, self.update_cluster_statistics)

  @staticmethod
  def _recursive_mkdir(path):
    try:
      os.makedirs(path)
    except OSError as exc:  # Python >2.5
      if exc.errno == errno.EEXIST and os.path.isdir(path):
        pass
      else:
        raise

  def run_submit_job(self, rw, header, message):
    '''Submit cluster job according to message.'''

    # Conditionally acknowledge receipt of the message
    txn = self._transport.transaction_begin()
    self._transport.ack(header, transaction=txn)

    parameters = rw.recipe_step['parameters']
    commands = parameters['cluster_commands']
    if not isinstance(commands, basestring):
      commands = "\n".join(commands)

    cluster = parameters.get('cluster')
    if cluster not in ('cluster', 'testcluster'):
      cluster = 'cluster'
    submission_params = parameters.get('cluster_submission_parameters', '')
    commands = commands.replace('$RECIPEPOINTER', str(rw.recipe_pointer))

    if 'recipefile' in parameters:
      recipefile = parameters['recipefile']
      self._recursive_mkdir(os.path.dirname(recipefile))
      self.log.debug("Writing recipe to %s", recipefile)
      commands = commands.replace('$RECIPEFILE', recipefile)
      with open(recipefile, 'w') as fh:
        fh.write(rw.recipe.pretty())
    if 'workingdir' in parameters:
      workingdir = parameters['workingdir']
      self._recursive_mkdir(workingdir)

    submission = [
      "module load global/" + cluster,
      "qsub %s << EOF" % submission_params,
      "#!/bin/bash",
      "cd " + workingdir,
      commands,
      "EOF"
    ]
    self.log.debug("Commands: %s", commands)
    self.log.debug("CWD: %s", parameters.get('workingdir'))
    self.log.debug(str(rw.recipe_step))
    result = run_process(["/bin/bash"], stdin = "\n".join(submission))
    assert result['exitcode'] == 0
    assert "has been submitted" in result['stdout']
    jobnumber = result['stdout'].split()[2]

    # Send results onwards
    rw.set_default_channel('default')
    rw.send_to('default', { 'jobid': jobnumber }, transaction=txn)

    self._transport.transaction_commit(txn)
    self.log.info("Submitted job %s to %s", str(jobnumber), cluster)

  def update_cluster_statistics(self):
    '''Gather some cluster statistics.'''
    submission = [
      "module load global/cluster",
      "qstat -f -r -u gda2 -xml"
    ]
    self.log.debug('Gathering cluster statistics...')
    result = run_process(["/bin/bash"], stdin = "\n".join(submission), print_stdout=False, print_stderr=False)
    if result['timeout']:
      self.log.warning('Timeout reading cluster statistics')
      return
    if result['exitcode']:
      self.log.warning('Encountered exit code %s reading cluster statistics', str(result['exitcode']))
      return
    self.log.debug('Received cluster statistics')

    cs = ClusterStatistics()
    joblist, queuelist = cs.parse_string(result['stdout'])
    self.log.debug('Parsed cluster statistics')

    pending_jobs = Counter(map(lambda j: j['queue'].split('@@')[0] if '@@' in j['queue'] else j['queue'], filter(lambda j: j['state'] == 'pending', joblist)))
    for queue in set(map(lambda q: q['class'], queuelist)) | set(pending_jobs):
      self.stats_log.debug("queuelevel: %d jobs waiting in queue %s", pending_jobs[queue], queue, extra={'jobqueue': queue})

    cluster_nodes = {}
    for q in queuelist:
      node_list = cluster_nodes.get(q['host'], [])
      node_list.append(q)
      cluster_nodes[q['host']] = node_list

    corestats = { 'total': 0, 'broken': 0, 'free_for_low': 0, 'free_for_medium': 0, 'free_for_high': 0 }
    corestats_admin =  { 'total': 0, 'broken': 0, 'free': 0 }
    for node in sorted(cluster_nodes):
      adminq = filter(lambda q: q['class'] == 'admin.q', cluster_nodes[node])
      if adminq:
        adminq_slots = adminq[0]['slots_total']
        corestats_admin['total'] += adminq_slots
        if adminq[0]['enabled'] and not adminq[0]['suspended']:
          corestats_admin['free'] += adminq[0]['slots_free']
        else:
          corestats_admin['broken'] += adminq_slots

    self.stats_log.debug("cluster statistics admin.q: %d total, %d broken, %d free cores", corestats_admin['total'], corestats_admin['broken'], corestats_admin['free'])
    self.stats_log.debug("cluster statistics general: %d total, %d broken, %d free-for-high, %d free-for-medium, %d free-for-low cores", corestats_admin['total'], corestats_admin['broken'], corestats_admin['free'], 0, 0)
