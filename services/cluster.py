from __future__ import absolute_import, division, print_function

import errno
import json
import logging
import os
import time
from collections import Counter

import dlstbx.util.cluster
import workflows.recipe
from dials.util.procrunner import run_process
from workflows.services.common_service import CommonService

class DLSCluster(CommonService):
  '''A service to interface zocalo with cluster functions. Here we can start
     new jobs and gather cluster statistics.'''

  # Human readable service name
  _service_name = "DLS Cluster service"

  # Logger name
  _logger_name = 'dlstbx.services.cluster'

  def __new__(cls, *args, **kwargs):
    '''Start DRMAA cluster control processes as children of the main process,
       and transparently inject references to those system-wide processes into
       all instantiated objects.'''
    if not hasattr(DLSCluster, '__drmaa_cluster'):
      setattr(DLSCluster, '__drmaa_cluster',
          dlstbx.util.cluster.Cluster('dlscluster'))
    if not hasattr(DLSCluster, '__drmaa_testcluster'):
      setattr(DLSCluster, '__drmaa_testcluster',
          dlstbx.util.cluster.Cluster('dlstestcluster'))
    instance = super(DLSCluster, cls).__new__(cls, *args, **kwargs)
    instance.__drmaa_cluster = getattr(DLSCluster, '__drmaa_cluster')
    instance.__drmaa_testcluster = getattr(DLSCluster, '__drmaa_testcluster')
    return instance

  def initializing(self):
    '''Subscribe to the cluster submission queue.
       Received messages must be acknowledged.'''
    self.log.info("Cluster service starting")

    workflows.recipe.wrap_subscribe(
      self._transport,
      'cluster.submission',
      self.run_submit_job,
      acknowledgement=True,
      log_extender=self.extend_log)

    # Generate cluster statistics up to every 30 seconds.
    # Statistics go with debug level to a separate logger so they can be
    # filtered by log monitors.
    self.cluster_statistics = dlstbx.util.cluster.ClusterStatistics()
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
    if 'recipeenvironment' in parameters:
      recipeenvironment = parameters['recipeenvironment']
      self._recursive_mkdir(os.path.dirname(recipeenvironment))
      self.log.debug("Writing recipe environment to %s", recipeenvironment)
      commands = commands.replace('$RECIPEENV', recipeenvironment)
      with open(recipeenvironment, 'w') as fh:
        json.dump(rw.environment, fh,
                  sort_keys=True, indent=2, separators=(',', ': '))
    if 'recipewrapper' in parameters:
      recipewrapper = parameters['recipewrapper']
      self._recursive_mkdir(os.path.dirname(recipewrapper))
      self.log.debug("Storing serialized recipe wrapper in %s", recipewrapper)
      commands = commands.replace('$RECIPEWRAP', recipewrapper)
      with open(recipewrapper, 'w') as fh:
        json.dump({ 'recipe': rw.recipe.recipe,
                    'recipe-pointer': rw.recipe_pointer,
                    'environment': rw.environment,
                    'recipe-path': rw.recipe_path,
                  }, fh,
                  sort_keys=True, indent=2, separators=(',', ': '))

    if 'workingdir' not in parameters or not parameters['workingdir'].startswith('/'):
      self.log.error("No absolute working directory specified. Will not run cluster job")
      self._transport.transaction_abort(txn)
      self._transport.nack(header)
      return
    workingdir = parameters['workingdir']
    self._recursive_mkdir(workingdir)

    submission = [
      ". /etc/profile.d/modules.sh",
      "module load global/" + cluster,
      "qsub %s << EOF" % submission_params,
      "#!/bin/bash",
      ". /etc/profile.d/modules.sh",
      "cd " + workingdir,
      commands,
      "EOF"
    ]
    self.log.debug("Commands: %s", commands)
    self.log.debug("CWD: %s", workingdir)
    self.log.debug(str(rw.recipe_step))
    result = run_process(["/bin/bash"], stdin = "\n".join(submission))
    assert result['exitcode'] == 0
    assert "has been submitted" in result['stdout']
    jobnumber = result['stdout'].split()[2]

    # Send results onwards
    rw.set_default_channel('job_submitted')
    rw.send({ 'jobid': jobnumber }, transaction=txn)

    self._transport.transaction_commit(txn)
    self.log.info("Submitted job %s to %s", str(jobnumber), cluster)

  def update_cluster_statistics(self):
    '''Gather some cluster statistics.'''
    self.log.debug('Gathering live cluster statistics...')
    timestamp = time.time()
    try:
      joblist, queuelist = self.cluster_statistics.run_on(
        self.__drmaa_cluster, arguments=['-f', '-r', '-u', 'gda2'])
    except AssertionError:
      self.log.error('Could not gather cluster statistics', exc_info=True)
      return
    self.calculate_cluster_statistics(joblist, queuelist, 'live', timestamp)

    # Now same for the testcluster
    self.log.debug('Gathering test cluster statistics...')
    timestamp = time.time()
    try:
      joblist, queuelist = self.cluster_statistics.run_on(
        self.__drmaa_testcluster, arguments=['-f', '-r', '-u', 'gda2'])
    except AssertionError:
      self.log.error('Could not gather test cluster statistics', exc_info=True)
      return
    self.calculate_cluster_statistics(joblist, queuelist, 'test', timestamp)

  def calculate_cluster_statistics(self, joblist, queuelist, cluster, timestamp):
    self.log.debug('Processing %s cluster statistics', cluster)
    pending_jobs = Counter(map(lambda j: j['queue'].split('@@')[0] if '@@' in j['queue'] else j['queue'],
                           filter(lambda j: j['state'] == 'pending' and 'h' not in j['statecode'], joblist)))
    waiting_jobs_per_queue = { queue: pending_jobs[queue] for queue in set(map(lambda q: q['class'], queuelist)) | set(pending_jobs) }
    self.report_statistic(waiting_jobs_per_queue, description='waiting-jobs-per-queue',
                          cluster=cluster, timestamp=timestamp)

    cluster_nodes = self.cluster_statistics.get_nodelist_from_queuelist(queuelist)
    node_summary = { node: self.cluster_statistics.summarize_node_status(status) for node, status in cluster_nodes.items() }
    self.report_statistic(node_summary, description='node-status',
                          cluster=cluster, timestamp=timestamp)

    corestats = {}
    corestats['cpu'] = { 'total': 0, 'broken': 0, 'free_for_low': 0, 'free_for_medium': 0, 'free_for_high': 0 }
    corestats['gpu'] = corestats['cpu'].copy()
    corestats['admin'] =  { 'total': 0, 'broken': 0, 'free': 0 }
    for nodename, node in cluster_nodes.iteritems():
      node = { q['class']: q for q in node }
      for queuename in list(node):
        if queuename.startswith('test'):
          if queuename.startswith('test-') and queuename[5:] not in node:
            node[queuename[5:]] = node[queuename]
          del node[queuename]

      if 'admin.q' in node:
        adminq_slots = node['admin.q']['slots_total']
        corestats['admin']['total'] += adminq_slots
        if node['admin.q']['enabled'] and not node['admin.q']['suspended'] and not node['admin.q']['error']:
          corestats['admin']['free'] += node['admin.q']['slots_free']
        else:
          corestats['admin']['broken'] += adminq_slots
        del node['admin.q']

      if not node:
        continue

      if (nodename.split('-')[2:3] or [None])[0] in ('com14',):
        nodetype='gpu'
      else:
        nodetype='cpu'
      cores = max(q['slots_total'] for q in node.values())
      corestats[nodetype]['total'] += cores
      node = { n: q for n, q in node.items() if q['enabled'] and not q['suspended'] and not q['error'] }
      if not node:
        corestats[nodetype]['broken'] += cores
        continue
      freelow, freemedium, freehigh = (node.get(q, {}).get('slots_free', 0) for q in ('low.q', 'medium.q', 'high.q'))
      corestats[nodetype]['free_for_low']    += freelow
      corestats[nodetype]['free_for_medium'] += max(freelow, freemedium)
      corestats[nodetype]['free_for_high']   += max(freelow, freemedium, freehigh)

    for nodetype in ('cpu', 'gpu'):
      corestats[nodetype]['used-high']   = corestats[nodetype]['total'] - corestats[nodetype]['broken'] - corestats[nodetype]['free_for_high']
      corestats[nodetype]['used-medium'] = corestats[nodetype]['free_for_high'] - corestats[nodetype]['free_for_medium']
      corestats[nodetype]['used-low']    = corestats[nodetype]['free_for_medium'] - corestats[nodetype]['free_for_low']
      for k, v in corestats[nodetype].items():
        corestats[k] = corestats.get(k, 0) + v
    corestats['admin']['used'] = corestats['admin']['total'] - corestats['admin']['free'] - corestats['admin']['broken']

    self.report_statistic(corestats, description='utilization',
                          cluster=cluster, timestamp=timestamp)

  def report_statistic(self, data, **kwargs):
    data_pack = {
      'statistic-group': 'cluster',
      'statistic': kwargs['description'],
      'statistic-cluster': kwargs['cluster'],
      'statistic-timestamp': kwargs['timestamp'],
    }
    data_pack.update(data)
    self._transport.broadcast('transient.statistics.cluster', data_pack)
    self._transport.send('statistics.cluster', data_pack, persistent=False)
