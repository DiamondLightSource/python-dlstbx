import glob
import json
import logging
import os
import shutil
from dials.util import procrunner
logger = logging.getLogger('dlstbx.wrap_autoPROC')

def get_rotation_axis_text(image):
  # look for rotation axis - FIXME this will not work right for full
  # imgCIF headers...

  text = open(image, 'r').read(2048)

  axis_text = ''

  for record in text.split('\n'):
    if record.startswith('# Oscillation_axis'):
      if 'SLOW' in record:
        axis_text = 'autoPROC_XdsKeyword_ROTATION_AXIS="0.0 -1.0 0.0"'
      return axis_text

  # should not get to here...
  return ''

def run(args):
  assert len(args) >= 2, len(args)
  recipe_pointer = args[0]
  recipe_file = args[1]
  assert os.path.isfile(recipe_file), recipe_file
  with open(recipe_file, 'rb') as f:
    recipe = json.load(f)

  params = recipe[recipe_pointer]['job_parameters']

  ap_env = {'autoPROC_HIGHLIGHT':'no'}

  image = params['autoPROC']['image']
  first, last = image.split(':')[1:]
  template = params['autoPROC']['template']
  prefix = template.split('_#')[0]
  ap_Id = '%s,%s,%s,%s,%s' % (prefix, os.path.split(image)[0], template,
                              first, last)

  # shouldn't need this any more:
  # 'StopIfDayOfTheWeekBeginsWithT="no"'
  ap_so_many_words = ['autoPROC_XdsKeyword_MAXIMUM_NUMBER_OF_PROCESSORS=20',
                      'StopIfSubdirExists="no"']

  axis_text = get_rotation_axis_text(image.split(':')[0])
  if axis_text:
    ap_so_many_words.append(axis_text)

  command = ['process', '-xml', '-Id', ap_Id, '-d',
             params['working_directory']]
  command.extend(ap_so_many_words)

  cwd = os.path.abspath(os.curdir)

  working_directory = params['working_directory']
  if not os.path.exists(working_directory):
    os.makedirs(working_directory)
  os.chdir(working_directory)
  logger.info('command: %s', ' '.join(command))
  logger.info('working directory: %s' % working_directory)
  result = procrunner.run_process(
    command, timeout=params.get('timeout'),
    print_stdout=False, print_stderr=False, environ=ap_env)

  logger.info('command: %s', ' '.join(result['command']))
  logger.info('timeout: %s', result['timeout'])
  logger.info('time_start: %s', result['time_start'])
  logger.info('time_end: %s', result['time_end'])
  logger.info('runtime: %s', result['runtime'])
  logger.info('exitcode: %s', result['exitcode'])
  logger.debug(result['stdout'])
  logger.debug(result['stderr'])

  # write autoPROC.log
  open(os.path.join(working_directory, 'autoPROC.log'), 'w').write(
    result['stdout'])

  # copy output files to result directory

  results_directory = params['results_directory']
  if not os.path.exists(results_directory):
    os.makedirs(results_directory)

  # FIXME decide what useful results should be copied over for a useful
  # fast_dp job

  for f in glob.glob(os.path.join(working_directory, '*.*')):
    shutil.copy(f, results_directory)

  os.chdir(cwd)


if __name__ == '__main__':
  logging.basicConfig(level=logging.DEBUG)
  import sys
  run(sys.argv[1:])
