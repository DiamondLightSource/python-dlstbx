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

  command = ('dials.import template=%s' % params['rlv']['template']).split()
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
  assert result['exitcode'] == 0

  # then find spots

  command = 'dials.find_spots datablock.json nproc=20'.split()
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
  assert result['exitcode'] == 0

  # broadcast a note to ActiveMQ subscribers?

  # them map to csv file

  command = 'dev.dials.csv datablock.json strong.pickle'.split()
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
  assert result['exitcode'] == 0

  # copy output files to result directory

  results_directory = params['results_directory']
  if not os.path.exists(results_directory):
    os.makedirs(results_directory)

  for f in glob.glob(os.path.join(working_directory, '*.*')):
    shutil.copy(f, results_directory)

  # make softlink to main results area without long name

  path_elements = results_directory.split(os.sep)
  link_path = os.path.join(path_elements[:-2] + [path_elements[-1]])
  os.symlink(results_directory, link_path)

  # FIXME record files in ISPyB

if __name__ == '__main__':
  logging.basicConfig(level=logging.DEBUG)
  import sys
  run(sys.argv[1:])
