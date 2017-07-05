from __future__ import absolute_import, division
from dials.util.procrunner import run_process
import logging
import os

class RRDTool(object):
  '''A wrapper around an rrdtool executable that does not rely on compiling
     rrdtool first.'''
  def __init__(self, basepath, rrdtool='rrdtool'):
    '''Create a wrapper instance. Pass a path in which the .rrd files will be
       stored in and the name of the rrdtool executable.'''
    self.basepath = basepath
    self.rrdtool = rrdtool
    self.log = logging.getLogger('dlstbx.util.rrdtool')
    if not os.path.isdir(basepath):
      raise IOError('rrdtool base directory %s does not exist' % basepath)

  def create_if_required(self, filename, options, start=1000000000):
    rrdfile = os.path.join(self.basepath, filename)
    if os.path.exists(rrdfile):
      return True
    command = [self.rrdtool, 'create', os.path.join(self.basepath, filename)]
    if start:
      command.extend(['--start', str(start)])
    command = ' '.join(command + options)
    return self._run_rrdtool(command)

  def update(self, filename, data):
    rrdfile = os.path.join(self.basepath, filename)
    command = [self.rrdtool, 'update', os.path.join(self.basepath, filename)]
    if isinstance(data, list):
      command.extend(data)
    else:
      command.append(data)
    return self._run_rrdtool(' '.join(command))

  def _run_rrdtool(self, command):
    stdin = "\n".join([
      "module load rrdtool",
      command
    ])

    result = run_process(["/bin/bash"],
                         stdin=stdin,
                         environ={'LD_LIBRARY_PATH':''})
    if result['exitcode'] or result['stderr']:
      self.log.warning('Command %s resulted in exitcode %d with error:\n%s', command, result['exitcode'], result['stderr'])
    else:
      self.log.debug('Successfully ran %s', command)
    return result['exitcode'] == 0
