from __future__ import absolute_import, division, print_function

import logging
import os

import dlstbx.zocalo.wrapper
import procrunner

logger = logging.getLogger('dlstbx.wrap.mosflm_strategy')

class MosflmStrategyWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']

    cwd = os.path.abspath(os.curdir)

    working_directory = os.path.abspath(params['working_directory'])
    results_directory = os.path.abspath(params['results_directory'])
    logger.info('working_directory: %s' %working_directory)
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)
    os.chdir(working_directory)

    image_directory = params['image_directory']
    image_pattern = params['image_pattern']
    image_first = int(params['image_first'])
    image_file_name = os.path.join(image_directory, image_pattern % image_first)
    commands = [
      'som.strategy', image_file_name]
    space_group = params.get('spacegroup')
    if space_group is not None:
      commands.append(space_group)
    result = procrunner.run_process(
      commands,
      timeout=params.get('timeout', 3600),
      print_stdout=True, print_stderr=True)

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    # insert results into database
    commands = [
      '/dls_sw/apps/mx-scripts/auto-edna/insertMosflmStrategies1.sh',
      params['dcid'],
      'strategy.dat'
    ]
    result = procrunner.run_process(
      commands,
      timeout=params.get('timeout', 3600),
      print_stdout=True, print_stderr=True)

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    beamline = params['beamline']
    if beamline in ('i03', 'i04'):
      result = self.run_xoalign(os.path.join(working_directory, 'mosflm_index.mat'))
    return result['exitcode'] == 0

  def run_xoalign(self, mosflm_index_mat):
    print(mosflm_index_mat)
    assert os.path.exists(mosflm_index_mat)
    params = self.recwrap.recipe_step['job_parameters']
    chi = params.get('chi')
    kappa = params.get('kappa')
    omega = params.get('omega')
    phi = params.get('phi')
    print(chi, kappa, omega, phi)
    if kappa is not None:
      datum="-D %s,%s,%s" % (phi, kappa, omega)
    elif chi is not None:
      datum="-D %s,%s,%s" % (phi, chi, omega)
    else:
      datum=""
    xoalign_py = '/dls_sw/apps/xdsme/graemewinter-xdsme/bin/Linux_i586/XOalign.py'
    commands = [xoalign_py, datum, mosflm_index_mat]
    print(' '.join(commands))
    result = procrunner.run_process(
      commands,
      timeout=params.get('timeout', 3600),
      print_stdout=True, print_stderr=True,
      environment={'XOALIGN_CALIB': '/dls_sw/%s/etc/xoalign_config.py' % params['beamline']},
    )

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    with open('XOalign.log', 'wb') as f:
      print(result['stdout'], file=f)
    self.insertXOalignStrategies(params['dcid'], 'XOalign.log')
    return result

  def insertXOalignStrategies(self, dcid, xoalign_log):

    assert os.path.isfile(xoalign_log)
    with open(xoalign_log, 'rb') as f:
      smargon = False
      found_solutions = False

      for line in f.readlines():
        if 'Independent Solutions' in line:
          found_solutions = True
          if 'SmarGon' in line:
            smargon = True
          continue

        if not found_solutions:
          continue

        kappa = None
        chi = None
        phi = None
        tokens = line.split()
        if len(tokens) < 4:
          continue

        solution_id = int(tokens[0])
        angles = [float(t) for t in tokens[1:3]]
        if smargon:
          chi, phi = angles
        else:
          kappa, phi = angles
        settings_str = ' '.join(tokens[3:]).replace("'", "")
        self.send_alignment_result_to_ispyb(
          dcid, 'XOalign', settings_str, 'XOalign %i' %solution_id,
          chi=chi, kappa=kappa, phi=phi)

  def send_alignment_result_to_ispyb(self,
    dcid, program, comments, short_comments,
    chi=None, kappa=None, phi=None):

    assert dcid > 0, 'Invalid data collection ID given.'
    assert [chi, kappa].count(None) == 1
    assert phi is not None
    if kappa is not None and kappa < 0:
      return # only insert strategies with positive kappa
    if chi is not None and (chi < 0 or chi > 45):
      return # only insert strategies with 0 < chi > 45
    if phi < 0:
      phi += 360 # make phi always positive
    if kappa is not None:
      kappa = '%.2f' %kappa
    elif chi is not None:
      chi = '%.2f' %chi
    phi = '%.2f' %phi

    result = {'dataCollectionId': dcid,
              'program': program,
              'shortComments': short_comments,
              'comments': comments,
              'phi': phi,
    }
    if kappa is not None:
      result['kappa'] = kappa
    elif chi is not None:
      result['chi'] = chi

    logger.debug('Inserting alignment result into ISPyB: %s' %str(result))
    self.recwrap.send_to('alignment-result', result)
