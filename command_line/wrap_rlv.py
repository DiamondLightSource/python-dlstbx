import glob
import json
import logging
import os
import shutil
from dials.util import procrunner
logger = logging.getLogger('dlstbx.wrap_rlv')

def run(args):
  assert len(args) >= 2, len(args)
  recipe_pointer = args[0]
  recipe_file = args[1]
  assert os.path.isfile(recipe_file), recipe_file
  with open(recipe_file, 'rb') as f:
    recipe = json.load(f)

  rlv_recipe = recipe[recipe_pointer]

  # first import the data to datablock.json
  params = rlv_recipe['job_parameters']
  working_directory = params['working_directory']
  if not os.path.exists(working_directory):
    os.makedirs(working_directory)
  os.chdir(working_directory)
  logger.info('working directory: %s' % working_directory)

  command = 'dials.import template=%s' % params['rlv']['template']
  logger.info('command: %s', ' '.join(command))
  result = procrunner.run_process(
    command, timeout=params.get('timeout'),
    print_stdout=False, print_stderr=False)
  logger.info('time_start: %s', result['time_start'])
  logger.info('time_end: %s', result['time_end'])
  logger.info('runtime: %s', result['runtime'])
  logger.info('exitcode: %s', result['exitcode'])
  logger.debug(result['stdout'])
  logger.debug(result['stderr'])

  # then find spots

  command = 'dials.import datablock.json nproc=20'
  logger.info('command: %s', ' '.join(command))
  result = procrunner.run_process(
    command, timeout=params.get('timeout'),
    print_stdout=False, print_stderr=False)
  logger.info('time_start: %s', result['time_start'])
  logger.info('time_end: %s', result['time_end'])
  logger.info('runtime: %s', result['runtime'])
  logger.info('exitcode: %s', result['exitcode'])
  logger.debug(result['stdout'])
  logger.debug(result['stderr'])

  # broadcast a note to ActiveMQ subscribers?

  # them map to csv file

  command = 'dev.dials.csv datablock.json strong.pickle'
  logger.info('command: %s', ' '.join(command))
  result = procrunner.run_process(
    command, timeout=params.get('timeout'),
    print_stdout=False, print_stderr=False)
  logger.info('time_start: %s', result['time_start'])
  logger.info('time_end: %s', result['time_end'])
  logger.info('runtime: %s', result['runtime'])
  logger.info('exitcode: %s', result['exitcode'])
  logger.debug(result['stdout'])
  logger.debug(result['stderr'])

  # copy output files to result directory

  results_directory = params['results_directory']
  if not os.path.exists(results_directory):
    os.makedirs(results_directory)

  for f in glob.glob(os.path.join(working_directory, '*.*')):
    shutil.copy(f, results_directory)

  os.chdir(cwd)


if __name__ == '__main__':
  logging.basicConfig(level=logging.DEBUG)
  import sys
  run(sys.argv[1:])
