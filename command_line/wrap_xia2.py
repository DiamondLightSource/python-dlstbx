import glob
import json
import logging
import os
import shutil
from dials.util import procrunner
logger = logging.getLogger('dlstbx.wrap_xia2')

def run(args):
  assert len(args) > 0
  recipe_file = args[0]
  assert os.path.isfile(recipe_file), recipe_file
  with open(recipe_file, 'rb') as f:
    recipe = json.load(f)

  xia2_recipe = recipe[str(recipe['start'][0][0])]

  # setup the xia2 command line

  command = ['xia2']
  params = xia2_recipe['job_parameters']
  for param, values in params['xia2'].iteritems():
    if param == 'images':
      params = 'images'
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

  logger.info(result['command'])
  logger.info(result['timeout'])
  logger.info(result['time_start'])
  logger.info(result['time_end'])
  logger.info(result['runtime'])
  logger.info(result['exitcode'])

  # copy output files to result directory

  result_directory = params['result_directory']

  for subdir in ('DataFiles', 'Harvest', 'LogFiles'):
    src = os.path.join(working_directory, subdir)
    dst = os.path.join(result_directory, subdir)
    logger.debug('Copying %s to %s' %(src, dst)
    shutil.copytree(src, dst)

  for f in glob.glob(os.path.join(working_directory, '*.*')):
    shutil.copy(f, result_directory)

  os.chdir(cwd)


if __name__ == '__main__':
  logging.basicConfig(level=logging.DEBUG)
  import sys
  run(sys.argv[1:])

