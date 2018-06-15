from __future__ import absolute_import, division, print_function

import logging
import os
import shutil

import dlstbx.util.symlink
import dlstbx.zocalo.wrapper
import procrunner

logger = logging.getLogger('dlstbx.wrap.rlv')

class RLVWrapper(dlstbx.zocalo.wrapper.BaseWrapper):
  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']

    # run in working directory
    working_directory = params['working_directory']
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)
    os.chdir(working_directory)

    command = ['dials.import', 'template=%s' % params['template']]
    logger.info('command: %s', ' '.join(command))
    result = procrunner.run(
      command, timeout=params.get('timeout'),
      print_stdout=False, print_stderr=False)
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])
    success = result['exitcode'] == 0

    if success:
      # then find spots

      command = ['dials.find_spots', 'datablock.json', 'nproc=20']
      logger.info('command: %s', ' '.join(command))
      result = procrunner.run(
        command, timeout=params.get('timeout'),
        print_stdout=False, print_stderr=False)
      logger.info('time_start: %s', result['time_start'])
      logger.info('time_end: %s', result['time_end'])
      logger.info('runtime: %s', result['runtime'])
      logger.info('exitcode: %s', result['exitcode'])
      logger.debug(result['stdout'])
      logger.debug(result['stderr'])
      success = result['exitcode'] == 0

    if success:
      # then map to csv file

      command = ['dev.dials.csv', 'dp=4', 'compress=true', 'csv=rl.csv.gz', 'datablock.json', 'strong.pickle']
      logger.info('command: %s', ' '.join(command))
      result = procrunner.run(
        command, timeout=params.get('timeout'),
        print_stdout=False, print_stderr=False)
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

    defaultfiles = ['rl.csv.gz']
    foundfiles = []
    for filename in params.get('keep_files', defaultfiles):
      if os.path.exists(filename):
        dst = os.path.join(results_directory, filename)
        logger.debug('Copying %s to %s' % (filename, dst))
        shutil.copy(filename, dst)
        foundfiles.append(dst)
        self.record_result_individual_file({
          'file_path': results_directory,
          'file_name': filename,
          'file_type': 'recip',
        })
      else:
        logger.warning('Expected output file %s missing', filename)
        success = False

    if foundfiles:
      logger.info('Notifying for found files: %s', str(foundfiles))
      self.record_result_all_files({ 'filelist': foundfiles })

    if params.get('results_symlink'):
      # Create symbolic link above working directory
      dlstbx.util.symlink.create_parent_symlink(results_directory, params['results_symlink'])

    logger.info('Done.')

    return success
