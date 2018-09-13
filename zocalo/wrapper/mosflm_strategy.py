from __future__ import absolute_import, division, print_function

import glob
import logging
import os
import shutil

import dlstbx.zocalo.wrapper
import procrunner

logger = logging.getLogger('dlstbx.wrap.mosflm_strategy')

class MosflmStrategyWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']

    cwd = os.path.abspath(os.curdir)

    working_directory = os.path.abspath(params['working_directory'])
    results_directory = os.path.abspath(params['results_directory'])
    logger.info('working_directory: %s' %working_directory)
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)
    os.chdir(working_directory)

    image_directory = params['image_directory']
    image_pattern = params['image_pattern']
    image_first = int(params['image_first'])
    image_file_name = os.path.join(image_directory, image_pattern % image_first)
    commands = [
      'som.strategy', image_file_name]
    space_group = params.get('spacegroup')
    if space_group is not None:
      commands.append(space_group)
    result = procrunner.run_process(
      commands,
      timeout=params.get('timeout', 3600),
      print_stdout=True, print_stderr=True)

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    # insert results into database
    commands = [
      '/dls_sw/apps/mx-scripts/auto-edna/insertMosflmStrategies1.sh',
      params['dcid'],
      'strategy.dat'
    ]
    result = procrunner.run_process(
      commands,
      timeout=params.get('timeout', 3600),
      print_stdout=True, print_stderr=True)

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    return result['exitcode'] == 0
