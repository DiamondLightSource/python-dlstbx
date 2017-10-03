from __future__ import absolute_import, division

import glob
import logging
import os
import shutil

import dlstbx.zocalo.wrapper
from dials.util import procrunner

logger = logging.getLogger('dlstbx.wrap.xia2_strategy')

from dlstbx.zocalo.wrapper.xia2 import Xia2Wrapper

class Xia2StrategyWrapper(Xia2Wrapper):

  def construct_commandline(self, params):
    '''Construct xia2 command line.
       Takes job parameter dictionary, returns array.'''

    command = ['xia2.strategy']

    for param, values in params['xia2.strategy'].iteritems():
      if param == 'images':
        param = 'image'
        values = values.split(',')
      if not isinstance(values, (list, tuple)):
        values = [values]
      if param == 'image':
        values = [v.split(':')[0] for v in values]
      for v in values:
        command.append('%s=%s' % (param, v))

    if params.get('ispyb_parameters'):
      if params['ispyb_parameters'].get('d_min'):
        command.append('xia2.settings.resolution.d_min=%s' % \
                       params['ispyb_parameters']['d_min'])
      if params['ispyb_parameters'].get('spacegroup'):
        command.append('xia2.settings.space_group=%s' % \
                       params['ispyb_parameters']['spacegroup'])
      if params['ispyb_parameters'].get('unit_cell'):
        command.append('xia2.settings.unit_cell=%s' % \
                       params['ispyb_parameters']['unit_cell'])

    command.append('strategy.phil')

    return command

  def send_results_to_ispyb(self):

    json_to_ispyb_columns = {
      'description': 'comments',
      'name': 'shortComments',
      'spacegroup': 'spacegroup',
      'resolution': 'resolution',
      'distance': None,
      'i_sigma': None,
      'completeness': 'completeness',
      'redundancy': None,
      'transmission': 'transmission',
      'total_exposure_time': None,
      'total_data_collection_time': None,
      'cell_a': 'unitCell_a',
      'cell_b': 'unitCell_b',
      'cell_c': 'unitCell_c',
      'cell_alpha': 'unitCell_alpha',
      'cell_beta': 'unitCell_beta',
      'cell_gamma': 'unitCell_gamma',
      'mosaicity': 'mosaicity',
      'phi_start': 'axisStart',
      'phi_end': 'axisEnd',
      'number_of_images': 'numberOfImages',
      'phi_width': 'oscillationRange',
      'exposure_time': 'exposureTime',
      'overlaps': None,
      'dmin': 'rankingResolution'
    }

    dcid = int(self.recwrap.recipe_step['job_parameters']['dcid'])
    assert dcid > 0, 'Invalid data collection ID given.'
    logger.info('Writing to data collection ID %s', str(dcid))
    json_file = 'strategy/strategies.json'
    assert os.path.isfile(json_file)
    import json
    with open(json_file, 'rb') as f:
      results_all = json.load(f)

    from dlstbx.ispybtbx import ispybtbx
    ispyb_conn = ispybtbx()
    for name, d in results_all.iteritems():
      screening_results = {}
      for src_k, dest_k in json_to_ispyb_columns.iteritems():
        if src_k in d:
          screening_results[dest_k] = d[src_k]

      screening_results['rotationAxis'] = 'omega'
      screening_results['shortComments'] = name
      screening_results['programVersion'] = 'xia2.strategy'
      screening_results['dataCollectionId'] = dcid
      screening_results['program'] = 'BEST'
      screening_results['wedgeNumber'] = '1'
      screening_results.setdefault('rankingResolution', 'NULL')
      ispyb_conn.insert_screening_results(dcid, screening_results)

    return
    # debugging code to interrogate results in database
    columns = ['Screening.programversion', 'Screening.comments']
    for s in ('a', 'b', 'c', 'alpha', 'beta', 'gamma'):
      columns.append('ScreeningOutputLattice.unitCell_%s' %s)
    for s in ('axisStart', 'axisEnd', 'oscillationRange', 'numberOfImages',
              'completeness', 'resolution', 'exposureTime'):
      columns.append('ScreeningStrategySubWedge.%s' %s)
    for r in ispyb_conn.get_screening_results([dcid], columns=columns):
      logger.info(r)


  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    command = self.construct_commandline(params)
    logger.info(command)

    # run xia2 in working directory

    cwd = os.path.abspath(os.curdir)

    working_directory = os.path.abspath(params['working_directory'])
    results_directory = os.path.abspath(params['results_directory'])
    logger.info('working_directory: %s' %working_directory)
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)
    os.chdir(working_directory)

    lifespan = params['strategy']['lifespan']
    transmission = float(params['strategy']['transmission'])
    wavelength = float(params['strategy']['wavelength'])
    beamline = params['strategy']['beamline']
    logger.info('transmission: %s' %transmission)
    logger.info('wavelength: %s' %wavelength)
    strategy_lifespan = round((lifespan * (100 / transmission)) * (wavelength/0.979)**-3, 0)
    gentle_strategy_lifespan = round((lifespan * (100 / transmission)) * (wavelength/0.979)**-3 / 10, 0)
    logger.info('lifespan: %s' %lifespan)

    if beamline == 'i24':
      min_exposure = 0.01
    elif beamline == 'i03':
      min_exposure = 0.01
    else:
      min_exposure = 0.04

    with open('strategy.phil', 'wb') as f:
      print >> f, """
strategy {
  name = "native"
  description = "Standard Native Dataset Multiplicity=3 I/sig=2 Maxlifespan=%(strategy_lifespan)ss"
  min_exposure = %(min_exposure)s
  multiplicity = 3.0
  i_over_sigi = 2.0
  max_total_exposure = %(strategy_lifespan)s
}
strategy {
  name = "anomalous"
  description = "Standard Anomalous Dataset Multiplicity=3 I/sig=2 Maxlifespan=%(strategy_lifespan)ss"
  min_exposure = %(min_exposure)s
  multiplicity = 3.0
  i_over_sigi = 2.0
  max_total_exposure = %(strategy_lifespan)s
  anomalous = True
}
strategy {
  name = "high multiplicity"
  description = "Strategy with target multiplicity=16 I/sig=2 Maxlifespan=%(strategy_lifespan)ss"
  min_exposure = %(min_exposure)s
  multiplicity = 16.0
  i_over_sigi = 2.0
  max_total_exposure = %(strategy_lifespan)s
  anomalous = True
}
strategy {
  name = "gentle"
  description = "Gentle: Target Multiplicity=2 I/sig=2 Maxlifespan=%(gentle_strategy_lifespan)ss"
  min_exposure = %(min_exposure)s
  multiplicity = 2.0
  i_over_sigi = 2.0
  max_total_exposure = %(gentle_strategy_lifespan)s
  anomalous = True
}
""" %dict(min_exposure=min_exposure,
          strategy_lifespan=strategy_lifespan,
          gentle_strategy_lifespan=gentle_strategy_lifespan)

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

    # copy output files to result directory

    if not os.path.exists(results_directory):
      os.makedirs(results_directory)

    for subdir in ('DataFiles', 'LogFiles', 'strategy'):
      src = os.path.join(working_directory, subdir)
      dst = os.path.join(results_directory, subdir)
      if os.path.exists(src):
        logger.debug('Copying %s to %s' % (src, dst))
        shutil.copytree(src, dst)
      else:
        logger.warning('Expected output directory does not exist: %s', src)

    for f in glob.glob(os.path.join(working_directory, '*.*')):
      shutil.copy(f, results_directory)

    os.chdir(results_directory)

    if os.path.exists('strategy/strategies.json'):
      logger.info('sending results to ispby')
      self.send_results_to_ispyb()
    else:
      logger.warning(
        'Expected output file does not exist: %s/strategy/strategies.json'
        %results_directory)

    os.chdir(cwd)

    return result['exitcode'] == 0
