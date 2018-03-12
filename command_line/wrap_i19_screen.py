from __future__ import absolute_import, division, print_function

import glob
import json
import logging
import os
import shutil

import procrunner

logger = logging.getLogger('dlstbx.wrap_i19_screen')

def run(args):
  assert len(args) >= 2, len(args)
  recipe_pointer = args[0]
  recipe_file = args[1]
  assert os.path.isfile(recipe_file), recipe_file
  with open(recipe_file, 'rb') as f:
    recipe = json.load(f)

  # construct i19.screen command line

  command = ['i19.screen']
  params = recipe[recipe_pointer]['job_parameters']
  command.append(params['screen-selection'])

  # got into working directory

  working_directory = params['working_directory']
  if not os.path.exists(working_directory):
    os.makedirs(working_directory)
  os.chdir(working_directory)

  # run i19.screen

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

  # copy output files to result directory?

  results_directory = params['results_directory']


if __name__ == '__main__':
  logging.basicConfig(level=logging.DEBUG)
  import sys
  run(sys.argv[1:])
