from __future__ import absolute_import, division, print_function

import logging
import os

import dlstbx.zocalo.wrapper
import procrunner

logger = logging.getLogger('dlstbx.wrap.snmct')

class SNMCTWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def construct_commandline(self, params):
    '''Construct snmct command line.
       Takes job parameter dictionary, returns array.'''

    command = ['xia2.multi_crystal_scale']

    for param, value in params['snmct'].iteritems():
      if param == 'data':
        for v in value:
          logging.info('Input file: %s' % v)
          command.append(v)
      else:
        logging.info('Parameter %s: %s' % (param, str(value)))
        command.append('%s=%s' % (param, value))

    return command

  def send_resuls_to_ispyb(self, json_file):
    from dlstbx.ispybtbx import ispybtbx
    ispyb_conn = ispybtbx()
    return

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    command = self.construct_commandline(params)

    # run SNMCT in working directory

    cwd = os.path.abspath(os.curdir)

    working_directory = params['working_directory']
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)
    os.chdir(working_directory)

    result = procrunner.run_process(
      command, timeout=params.get('timeout'),
      print_stdout=True, print_stderr=True)

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    os.chdir(cwd)

    return result['exitcode'] == 0
