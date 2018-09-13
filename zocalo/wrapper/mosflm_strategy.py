from __future__ import absolute_import, division, print_function

import glob
import logging
import os
import shutil

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
    #os.environ['BEAMLINE'] = params['beamline'] # this needs setting before a call to module load xdsme/graeme
    os.environ['XOALIGN_CALIB'] = '/dls_sw/%s/etc/xoalign_config.py' % params['beamline']
    xoalign_py = '/dls_sw/apps/xdsme/graemewinter-xdsme/bin/Linux_i586/XOalign.py'
    commands = [xoalign_py, datum, mosflm_index_mat]
    print(' '.join(commands))
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

    with open('XOalign.log', 'wb') as f:
      print(result['stdout'], file=f)
    self.insertXOalignStrategies(params['dcid'], 'XOalign.log')
    return result

  @staticmethod
  def insertXOalignStrategies(dcid, xoalign_log):

    def insert_alignment_result(conn, dcid, program, comments, short_comments,
                                chi=None, kappa=None, phi=None):
      assert phi is not None
      assert [chi, kappa].count(None) == 1

      mx_screening = conn.mx_screening
      screening_params = mx_screening.get_screening_params()

      screening_params['dcid'] = dcid
      screening_params['program_version'] = program
      screening_params['comments'] = comments
      screening_params['short_comments'] = short_comments

      screeningId = mx_screening.insert_screening(list(screening_params.values()))
      assert screeningId is not None

      output_params = mx_screening.get_screening_output_params()
      output_params['screening_id'] = screeningId
      #output_params['alignment_success'] = 1 ???
      screeningOutputId = mx_screening.insert_screening_output(list(output_params.values()))
      assert screeningOutputId is not None

      strategy_params = mx_screening.get_screening_strategy_params()
      strategy_params['screening_output_id'] = screeningOutputId
      strategy_params['program'] = program
      screeningStrategyId = mx_screening.insert_screening_strategy(list(strategy_params.values()))
      assert screeningStrategyId is not None

      wedge_params = mx_screening.get_screening_strategy_wedge_params()
      wedge_params['screening_strategy_id'] = screeningStrategyId
      wedge_params['chi'] = chi
      wedge_params['kappa'] = kappa
      wedge_params['phi'] = phi
      screeningStrategyWedgeId = mx_screening.insert_screening_strategy_wedge(list(wedge_params.values()))
      assert screeningStrategyWedgeId is not None

      #sub_wedge_params = mx_screening.get_screening_strategy_sub_wedge_params_params()

    assert os.path.isfile(xoalign_log)
    with open(xoalign_log, 'rb') as f:

      smargon = False
      found_solutions = False

      import ispyb
      ispyb_config = os.getenv('ISPYB_CONFIG_FILE')
      with ispyb.open(ispyb_config) as conn:

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

            if kappa is not None and kappa < 0:
              continue # only insert strategies with positive kappa
            if chi is not None and (chi < 0 or chi > 45):
              continue # only insert strategies with 0 < chi > 45
            if phi < 0:
              phi += 360 # make phi always positive
            if kappa is not None:
              kappa = '%.3f' %kappa
            elif chi is not None:
              chi = '%.3f' %chi
            phi = '%.3f' %phi

            insert_alignment_result(
              conn, dcid, 'XOalign', settings_str, 'XOalign %i' %solution_id,
              chi=chi, kappa=kappa, phi=phi)
