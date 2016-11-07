import glob
import json
import logging
import os
import shutil
from dials.util import procrunner
logger = logging.getLogger('dlstbx.wrap_multi_xia2')

def run(args):
  assert len(args) >= 2, len(args)
  recipe_pointer = args[0]
  recipe_file = args[1]
  assert os.path.isfile(recipe_file), recipe_file
  with open(recipe_file, 'rb') as f:
    recipe = json.load(f)

  multi_xia2_recipe = recipe[recipe_pointer]

  # setup the multi_xia2 command line

  command = ['xia2']
  params = multi_xia2_recipe['job_parameters']
  for param, values in params['multi_xia2'].iteritems():
    if param == 'images':
      param = 'image'
      values = values.split(',')
    if not isinstance(values, (list, tuple)):
      values = [values]
    for v in values:
      command.append('%s=%s' %(param, v))

  # run xia2 in working directory

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

  # copy output files to result directory

  results_directory = params['results_directory']
  if not os.path.exists(results_directory):
    os.makedirs(results_directory)

  for subdir in ('DataFiles', 'Harvest', 'LogFiles'):
    src = os.path.join(working_directory, subdir)
    dst = os.path.join(results_directory, subdir)
    if os.path.exists(src):
      logger.debug('Copying %s to %s' %(src, dst))
      shutil.copytree(src, dst)
    else:
      logger.warn('Expected output directory does not exist: %s', src)

  for f in glob.glob(os.path.join(working_directory, '*.*')):
    shutil.copy(f, results_directory)

  os.chdir(cwd)


if __name__ == '__main__':
  logging.basicConfig(level=logging.DEBUG)
  import sys
  run(sys.argv[1:])

