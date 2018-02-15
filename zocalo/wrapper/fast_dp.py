from __future__ import absolute_import, division, print_function

import json
import logging
import os

import dlstbx.zocalo.wrapper
from dials.util import procrunner

logger = logging.getLogger('dlstbx.wrap.fast_dp')

class FastDPWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def construct_commandline(self, params):
    '''Construct fast_dp command line.
       Takes job parameter dictionary, returns array.'''

    command = ['fast_dp']

    command.append(params['fast_dp']['filename'])

    if params.get('ispyb_parameters'):
      if params['ispyb_parameters'].get('d_min'):
        command.append('--resolution-low=%s' % params['ispyb_parameters']['d_min'])
      if params['ispyb_parameters'].get('spacegroup'):
        command.append('--spacegroup.space_group=%s' % params['ispyb_parameters']['spacegroup'])
      if params['ispyb_parameters'].get('unit_cell'):
        command.append('--cell=%s' % params['ispyb_parameters']['unit_cell'])

    return command

  def send_resuls_to_ispyb(self, json_file):
    return # dummy function for now

#   from dlstbx.ispybtbx import ispybtbx
#   ispyb_conn = ispybtbx()
#
    with open(json_file, 'rb') as f:
      ispyb_data = json.load(f)
#   logger.debug('Inserting fast_dp results into ISPyB: %s' % str(ispyb_data))
#   ispyb_conn.insert_fastep_phasing_results(ispyb_data)

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    command = self.construct_commandline(params)

    # run fast_dp in working directory
    working_directory = params['working_directory']
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)
    os.chdir(working_directory)

    result = procrunner.run_process(
      command, timeout=params.get('timeout'),
      print_stdout=False, print_stderr=False)

    if os.path.exists('fast_dp.error'):
      # fast_dp anomaly: exit code 0 and no stderr output still means failure if error file exists
      result['exitcode'] = 1

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

    allfiles = []
    for filename in ('fast_dp.log', 'fast_dp.error'):
      dst = os.path.join(results_directory, filename)
      if os.path.exists(filename):
        logger.debug('Copying %s to %s' % (filename, dst))
        shutil.copy(filename, dst)
        allfiles.append(dst)
        self.record_result_individual_file({
          'file_path': results_directory,
          'file_name': filename,
          'file_type': 'log',
        })

    json_file = 'fast_dp.json'
    if os.path.exists(json_file):
      logger.info('Sending fast_dp results to ISPyB')
      self.send_results_to_ispyb(json_file)
    else:
      logger.warning('Expected output file %s does not exist' % json_file)

    if allfiles:
      self.record_result_all_files({ 'filelist': allfiles })

    return result['exitcode'] == 0
