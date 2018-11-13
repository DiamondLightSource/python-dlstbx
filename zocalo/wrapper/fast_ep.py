from __future__ import absolute_import, division, print_function

import logging
import os

import dlstbx.zocalo.wrapper
import procrunner

logger = logging.getLogger('dlstbx.wrap.fast_ep')

class FastEPWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def construct_commandline(self, params):
    '''Construct fast_ep command line.
       Takes job parameter dictionary, returns array.'''

    command = ['fast_ep']
    mtz = None
    if params.get('ispyb_parameters'):
      if params['ispyb_parameters'].get('data'):
        mtz = os.path.abspath(params['ispyb_parameters'].get('data'))
    if mtz is None:
      mtz = os.path.abspath(params['fast_ep']['data'])
    assert mtz is not None
    self._mtz = mtz
    command.append('data=%s' % mtz)

    for param, value in params['fast_ep'].iteritems():
      if param == 'data':
        continue
      logging.info('Parameter %s: %s' % (param, str(value)))
      if param == 'rlims':
        value = ','.join([str(r) for r in value])
      command.append('%s=%s' % (param, value))
    command.append('xml=fast_ep.xml')

    return command

  def send_results_to_ispyb(self, xml_file):
    params = self.recwrap.recipe_step['job_parameters']
    command = [
      'python',
      '/dls_sw/apps/mx-scripts/dbserver/src/phasing2ispyb.py',
      '-s', 'sci-serv3', '-p', '1994', '--fix_sgids', '-d',
      '-i', xml_file,
      '-f', self._mtz,
      '-o', os.path.join(params['working_directory'], 'fast_ep_ispyb_ids.xml')
    ]

    result = procrunner.run_process(
      command, timeout=params.get('timeout'),
      print_stdout=True, print_stderr=True,
      working_directory=params['working_directory'])

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    return result['exitcode'] == 0

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    command = self.construct_commandline(params)

    # run fast_ep in working directory
    working_directory = params['working_directory']
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)

    result = procrunner.run_process(
      command, timeout=params.get('timeout'),
      print_stdout=False, print_stderr=False,
      working_directory=working_directory)

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    xml_file = os.path.join(working_directory, 'fast_ep.xml')
    if os.path.exists(xml_file):
      logger.info('Sending fast_ep phasing results to ISPyB')
      self.send_results_to_ispyb(xml_file)
    else:
      logger.warning(
        'Expected output file does not exist: %s' % xml_file)


    return result['exitcode'] == 0
