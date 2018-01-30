from __future__ import absolute_import, division, print_function

import logging
import os

import dlstbx.zocalo.wrapper
from dials.util import procrunner

logger = logging.getLogger('dlstbx.wrap.fast_ep')

class FastEPWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def construct_commandline(self, params):
    '''Construct fast_ep command line.
       Takes job parameter dictionary, returns array.'''

    command = ['fast_ep']

    for param, value in params['fast_ep'].iteritems():
      logging.info('Parameter %s: %s' % (param, str(value)))
      if param == 'rlims':
        value = ','.join([str(r) for r in value])
      command.append('%s=%s' % (param, value))

    return command

  def send_resuls_to_ispyb(self, json_file):
    from dlstbx.ispybtbx import ispybtbx
    ispyb_conn = ispybtbx()

    with open(json_file, 'rb') as f:
      import json
      ispyb_data = json.load(f)
    logger.debug('Inserting fast_ep phasing results into ISPyB: %s' % str(ispyb_data))
    ispyb_conn.insert_fastep_phasing_results(ispyb_data)

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    command = self.construct_commandline(params)

    # run fast_ep in working directory

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

    json_file = os.path.join(working_directory, params['fast_ep']['json'])
    if os.path.exists(json_file):
      logger.info('Sending fast_ep phasing results to ISPyB')
      self.send_results_to_ispyb(json_file)
    else:
      logger.warning(
        'Expected output file does not exist: %s' % json_file)

    os.chdir(cwd)

    return result['exitcode'] == 0
