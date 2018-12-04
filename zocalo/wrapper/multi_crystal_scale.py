from __future__ import absolute_import, division, print_function

import logging
import os

import procrunner
import zocalo.wrapper

logger = logging.getLogger('dlstbx.wrap.multi_crystal_scale')

class MultiCrystalScaleWrapper(zocalo.wrapper.BaseWrapper):
  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']

    # run in working directory
    working_directory = params['working_directory']
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)
    os.chdir(working_directory)

    # construct multi_crystal_scale command line
    command = ['xia2.multi_crystal_scale_and_merge']
    assert params['data_files'] is not None
    assert len(params['data_files']) > 1
    for (experiments, reflections) in params['data_files']:
      command.append(experiments)
      command.append(reflections)
    if 'nproc' in params:
      command.append('nproc=%s' % params['nproc'])

    # run xia2.multi_crystal_scale_and_merge
    result = procrunner.run(
      command, timeout=params.get('timeout'),
      print_stdout=True, print_stderr=True)

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])
    success = result['exitcode'] == 0

    # copy output files to result directory
    results_directory = params['results_directory']
    if not os.path.exists(results_directory):
      os.makedirs(results_directory)

    logger.info('Done.')

    return success
