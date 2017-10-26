from __future__ import absolute_import, division, print_function

import glob
import logging
import os
import shutil

import dlstbx.zocalo.wrapper
from dials.util import procrunner

logger = logging.getLogger('dlstbx.wrap.xia2')

class Xia2Wrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def construct_commandline(self, params):
    '''Construct xia2 command line.
       Takes job parameter dictionary, returns array.'''

    command = ['xia2']

    for param, values in params['xia2'].iteritems():
      if param == 'images':
        param = 'image'
        values = values.split(',')
      if not isinstance(values, (list, tuple)):
        values = [values]
      for v in values:
        command.append('%s=%s' % (param, v))

    if params.get('ispyb_parameters'):
      if params['ispyb_parameters'].get('d_min'):
        command.append('xia2.settings.resolution.d_min=%s' % \
                       params['ispyb_parameters']['d_min'])
      if params['ispyb_parameters'].get('spacegroup'):
        command.append('xia2.settings.space_group=%s' % \
                       params['ispyb_parameters']['spacegroup'])
      if params['ispyb_parameters'].get('unit_cell'):
        command.append('xia2.settings.unit_cell=%s' % \
                       params['ispyb_parameters']['unit_cell'])

    return command

  def send_results_to_ispyb(self):
    logger.debug("Reading xia2 results")
    from xia2.command_line.ispyb_json import ispyb_object

    message = ispyb_object()
    source = os.path.join(os.getcwd(), 'xia2.txt')

    def recursive_replace(thing, old, new):
      '''Recursive string replacement in data structures.'''

      def _recursive_apply(item):
        '''Internal recursive helper function.'''
        if isinstance(item, basestring):
          return item.replace(old, new)
        if isinstance(item, dict):
          return { _recursive_apply(key): _recursive_apply(value) for
                   key, value in item.items() }
        if isinstance(item, tuple):
          return tuple(_recursive_apply(list(item)))
        if isinstance(item, list):
          return [ _recursive_apply(x) for x in item ]
        return item
      return _recursive_apply(thing)

    logger.debug("Replacing temporary zocalo paths with correct destination paths")
    message = recursive_replace(
        message,
        self.recwrap.recipe_step['job_parameters']['working_directory'],
        self.recwrap.recipe_step['job_parameters']['results_directory']
      )

    dcid = int(self.recwrap.recipe_step['job_parameters']['dcid'])
    assert dcid > 0, "Invalid data collection ID given."
    logger.debug("Writing to data collection ID %s", str(dcid))
    for container in message['AutoProcScalingContainer']['AutoProcIntegrationContainer']:
      container['AutoProcIntegration']['dataCollectionId'] = dcid

    # Use existing AutoProcProgramID
    if self.recwrap.environment.get('ispyb_autoprocprogram_id'):
      message['AutoProcProgramContainer']['AutoProcProgram'] = \
        self.recwrap.environment['ispyb_autoprocprogram_id']

    logger.debug("Sending %s", str(message))
    self.recwrap.transport.send('ispyb', message)

    logger.info("Processing information from %s attached to data collection %s", source, str(dcid))

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    command = self.construct_commandline(params)

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

    for subdir in ('DataFiles', 'LogFiles'):
      src = os.path.join(working_directory, subdir)
      dst = os.path.join(results_directory, subdir)
      if os.path.exists(src):
        logger.debug('Copying %s to %s' % (src, dst))
        shutil.copytree(src, dst)
      else:
        logger.warning('Expected output directory does not exist: %s', src)

    for f in glob.glob(os.path.join(working_directory, '*.*')):
      shutil.copy(f, results_directory)

    os.chdir(results_directory)

    if os.path.exists('xia2.json'):
      self.send_results_to_ispyb()

    os.chdir(cwd)

    return result['exitcode'] == 0
