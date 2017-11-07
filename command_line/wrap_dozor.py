from __future__ import absolute_import, division, print_function

import glob
import json
import logging
import os
import shutil

from dials.util import procrunner

logger = logging.getLogger('dlstbx.wrap_dozor')

def run(args):
  assert len(args) >= 2, len(args)
  recipe_pointer = args[0]
  recipe_file = args[1]
  assert os.path.isfile(recipe_file), recipe_file
  with open(recipe_file, 'rb') as f:
    recipe = json.load(f)

  dozor_recipe = recipe[recipe_pointer]

  # read recipe
  # if needed: create working directory
  # go there; create dozor.in
  # run dozor
  # extract output => data stucture
  # write to database

  command = ['dozor', 'dozor.in']
  filename = None
  params = dozor_recipe['job_parameters']
  for param, values in params['dozor'].iteritems():
    if param == 'image':
      tokens = values.split(':')
      filename = tokens[0]
      start, end = int(tokens[1]), int(tokens[2])
      command.extend(['-1', str(start), '-N', str(end)])

  assert not filename is None
  command.append(filename)
  # run dozor in working directory

  cwd = os.path.abspath(os.curdir)

  working_directory = params['working_directory']
  if not os.path.exists(working_directory):
    os.makedirs(working_directory)
  os.chdir(working_directory)
  logger.info('command: %s', ' '.join(command))
  logger.info('working directory: %s' % working_directory)
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
