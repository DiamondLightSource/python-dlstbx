from __future__ import absolute_import, division, print_function

import logging
import os
from datetime import datetime

import dlstbx.zocalo.wrapper
from dials.util import procrunner

logger = logging.getLogger('dlstbx.wrap.big_ep')

class BigEPWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def construct_commandline(self, params):
    '''Construct big_ep command line.
       Takes job parameter dictionary, returns array.'''

    command = ['sh', '/dls_sw/apps/mx-scripts/auto-big-ep/zoc-bigep.sh',
               params['big_ep']['autoproc_id'],
               '%4d%02d%02d_%02d%02d%02d' % tuple(datetime.now().timetuple()[:6]),
               params['big_ep']['beamline']]

    return command

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    command = self.construct_commandline(params)

    # run big_ep in working directory

    cwd = os.path.abspath(os.curdir)

    working_directory = params['working_directory']
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)
    os.chdir(working_directory)

    result = procrunner.run_process(
      command, timeout=params.get('timeout'),
      print_stdout=False, print_stderr=False)

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    os.chdir(cwd)

    return result['exitcode'] == 0
