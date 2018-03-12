from __future__ import absolute_import, division, print_function

import logging
import os
import shutil

import dlstbx.zocalo.wrapper
from procrunner import run_process

logger = logging.getLogger('dlstbx.wrap.spot_counts_per_image')

class SCPIWrapper(dlstbx.zocalo.wrapper.BaseWrapper):
  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']

    # run in working directory
    working_directory = params['working_directory']
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)
    os.chdir(working_directory)

    prefix = 'pia'

    if os.getenv('NSLOTS') or params.get('nproc'):
      nproc = [ "nproc=" + str(os.getenv('NSLOTS') or params.get('nproc')) ]
    else:
      nproc = []

    for command in (
          ['dials.import', params['data'] ],
          ['dials.find_spots', 'datablock.json'] + nproc,
          ['dials.spot_counts_per_image', 'datablock.json', 'strong.pickle', 'json=%s.json' % prefix, 'split_json=True'],
        ):
      result = run_process(
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
      if result['exitcode'] != 0:
        break

    success = result['exitcode'] == 0

    # copy output files to result directory
    results_directory = params['results_directory']
    if not os.path.exists(results_directory):
      os.makedirs(results_directory)

    defaultfiles = ('estimated_d_min', 'n_spots_total')
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
      self.record_result_all_files({ 'filelist': foundfiles })

    return success
