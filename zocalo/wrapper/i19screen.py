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
    result = procrunner.run(
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

    defaultfiles = ['i19.screen.log']
    if os.path.exists('indexed.pickle'):
      defaultfiles.append('indexed.pickle')
      defaultfiles.append('experiments.json')
      defaultfiles.append('dials-report.html')
    elif os.path.exists('strong.pickle'):
      defaultfiles.append('strong.pickle')
      defaultfiles.append('datablock.json')
      if os.path.exists('all_spots.pickle'):
        defaultfiles.append('all_spots.pickle')

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
          'file_type': 'log' if filename.endswith('.log') or filename.endswith('.html') else 'result',
        })
      else:
        logger.warning('Expected output file %s missing', filename)
        success = False

    if foundfiles:
      logger.info('Notifying for found files: %s', str(foundfiles))
      self.record_result_all_files({ 'filelist': foundfiles })

    if params.get('results_symlink'):
      # Create symbolic link above working directory
      path_elements = results_directory.split(os.sep)

      # Full path to the symbolic link
      link_path = os.sep.join(path_elements[:-2] + [params['results_symlink']])

      # Only write symbolic link if a symbolic link is created or overwritten
      # Do not overwrite real files, do not touch real directories
      if not os.path.exists(link_path) or os.path.islink(link_path):
        # because symlink can't be overwritten, create a temporary symlink in the child directory
        # and then rename on top of potentially existing one in the parent directory.
        tmp_link = os.sep.join(path_elements[:-1] + ['.tmp.' + params['results_symlink']])
        os.symlink(os.sep.join(path_elements[-2:]), tmp_link)
        os.rename(tmp_link, link_path)

    logger.info('Done.')

    return success
