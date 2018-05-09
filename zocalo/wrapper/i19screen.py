from __future__ import absolute_import, division, print_function

import logging
import os
import shutil

import dlstbx.zocalo.wrapper
import procrunner

logger = logging.getLogger('dlstbx.wrap.i19_screen')

class I19ScreenWrapper(dlstbx.zocalo.wrapper.BaseWrapper):
  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']

    # run in working directory
    working_directory = params['working_directory']
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)
    os.chdir(working_directory)

    # construct i19.screen command line
    command = ['i19.screen']
    command.append(params['screen-selection'])

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
    success = result['exitcode'] == 0

    # copy output files to result directory
    results_directory = params['results_directory']
    if not os.path.exists(results_directory):
      os.makedirs(results_directory)

    defaultfiles = ('i19.screen.log', 'dials-report.html',
                    'experiments_with_profile_model.json', 'predicted.pickle')
    foundfiles = []
    for filename in params.get('keep_files', defaultfiles):
      filename = prefix + '_' + filename + '.json'

      if os.path.exists(filename):
        dst = os.path.join(results_directory, filename)
        logger.debug('Copying %s to %s' % (filename, dst))
        shutil.copy(filename, dst)
        foundfiles.append(dst)
        self.record_result_individual_file({
          'file_path': results_directory,
          'file_name': filename,
          'file_type': 'pia',
        })
      else:
        logger.warning('Expected output file %s missing', filename)
        success = False

    if foundfiles:
      logger.info('Notifying for found files: %s', str(foundfiles))
      self.record_result_all_files({ 'filelist': foundfiles })

    logger.info('Done.')

    return success
