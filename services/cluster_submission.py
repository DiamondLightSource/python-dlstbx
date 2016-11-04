from __future__ import absolute_import, division
from dials.util.procrunner import run_process
import errno
import os
import random
import string
from workflows.recipe import Recipe
from workflows.services.common_service import CommonService

class DLSClusterSubmission(CommonService):
  '''A service to run xia2 processing.'''

  # Human readable service name
  _service_name = "DLS cluster submitter"

  def initializing(self):
    '''Subscribe to the cluster submission queue.
       Received messages must be acknowledged.'''
    self._transport.subscribe('cluster.submission',
      self.run_submit_job,
      acknowledgement=True)

  @staticmethod
  def _recursive_mkdir(path):
    try:
      os.makedirs(path)
    except OSError as exc:  # Python >2.5
      if exc.errno == errno.EEXIST and os.path.isdir(path):
        pass
      else:
        raise

  def run_submit_job(self, header, message):
    '''Submit cluster job according to message.'''

    # Conditionally acknowledge receipt of the message
    txn = self._transport.transaction_begin()
    self._transport.ack(header['message-id'], transaction=txn)

    current_recipe = Recipe(header['recipe'])
    current_recipepointer = int(header['recipe-pointer'])
    subrecipe = current_recipe[current_recipepointer]
    parameters = subrecipe['parameters']
    commands = parameters['cluster_commands']
    if not isinstance(commands, basestring):
      commands = "\n".join(commands)

    cluster = parameters.get('cluster', 'cluster') # is ignored though
    submission_params = parameters.get('cluster_submission_parameters', '')
    commands = commands.replace('$RECIPEPOINTER', str(int(header['recipe-pointer'])))

    if 'recipefile' in parameters:
      recipefile = parameters['recipefile']
      self._recursive_mkdir(os.path.dirname(recipefile))
      print "Write recipe to ", recipefile
      commands = commands.replace('$RECIPEFILE', recipefile)
      with open(recipefile, 'w') as fh:
        fh.write(header['recipe'])
    if 'workingdir' in parameters:
      workingdir = parameters['workingdir']
      self._recursive_mkdir(workingdir)

    submission = [
      "module load global/testcluster",
      "qsub %s << EOF" % submission_params,
      "#!/bin/bash",
      "cd " + workingdir,
      commands,
      "EOF"
    ]
    print "Commands: ", commands
    print "CWD: ", parameters.get('workingdir')
    print subrecipe
    result = run_process(["/bin/bash"], stdin = "\n".join(submission))
    assert result['exitcode'] == 0
    assert "has been submitted" in result['stdout']
    jobnumber = result['stdout'].split()[2]

    # Send results onwards
    new_header = { 'recipe': header['recipe'] }
    results = { 'jobid': jobnumber }
    self._transport.send('transient.destination', results, transaction=txn, headers=new_header)
    self._transport.transaction_commit(txn)
