from __future__ import absolute_import, division, print_function

import logging
import py

import dlstbx.util.symlink
import procrunner
import zocalo.wrapper

logger = logging.getLogger('dlstbx.wrap.i19_screen')

class I19ScreenWrapper(zocalo.wrapper.BaseWrapper):
  def run(self):
    assert hasattr(self, 'recwrap'), "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    working_directory = py.path.local(params['working_directory'])
    results_directory = py.path.local(params['results_directory'])

    # create working directory
    working_directory.ensure(dir=True)
    if params.get('create_symlink'):
      # Create symbolic link above working directory
      dlstbx.util.symlink.create_parent_symlink(working_directory.strpath, params['create_symlink'])

    # construct i19.screen command line
    command = ['i19.screen', params['screen-selection']]

    # run i19.screen
    result = procrunner.run(
        command, timeout=params.get('timeout'),
        working_directory=working_directory.strpath,
        environment_override={'PYTHONIOENCODING': 'UTF-8'},
    )
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
    results_directory.ensure(dir=True)

    defaultfiles = ['i19.screen.log']
    if working_directory.join('indexed.pickle').check():
      defaultfiles.append('indexed.pickle')
      defaultfiles.append('experiments.json')
      if working_directory.join('dials-report.html').check():
        defaultfiles.append('dials-report.html')
    elif working_directory.join('strong.pickle').check():
      defaultfiles.append('strong.pickle')
      defaultfiles.append('datablock.json')
      if working_directory.join('all_spots.pickle').check():
        defaultfiles.append('all_spots.pickle')

    foundfiles = []
    for filename in params.get('keep_files', defaultfiles):
      if working_directory.join(filename).check():
        dst = results_directory.join(filename)
        logger.debug('Copying %s to %s' % (filename, dst.strpath))
        working_directory.join(filename).copy(dst)
        foundfiles.append(dst.strpath)
        self.record_result_individual_file({
          'file_path': dst.dirname,
          'file_name': dst.basename,
          'file_type': 'log' if filename.endswith('.log') or filename.endswith('.html') else 'result',
        })
      else:
        logger.warning('Expected output file %s missing', filename)
        success = False

    if foundfiles:
      logger.info('Notifying for found files: %s', str(foundfiles))
      self.record_result_all_files({ 'filelist': foundfiles })

    if params.get('create_symlink'):
      # Create symbolic link above results directory
      dlstbx.util.symlink.create_parent_symlink(results_directory.strpath, params['create_symlink'])

    logger.info('Done.')

    return success
