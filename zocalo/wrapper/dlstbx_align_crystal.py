from __future__ import absolute_import, division, print_function

import json
import logging
import os
import shutil

import dlstbx.util.symlink
from dlstbx.util.merging_statistics import get_merging_statistics
import procrunner
import py
import zocalo.wrapper

from dxtbx.serialize import load

logger = logging.getLogger('dlstbx.wrap.dlstbx.align_crystal')

class AlignCrystalWrapper(zocalo.wrapper.BaseWrapper):

  def insert_dials_align_strategies(
      self, dcid, crystal_symmetry, results):
    solutions = results['solutions']
    gonio = results['goniometer']
    axis_names = gonio['names']

    kappa_name = None
    chi_name = None
    phi_name = None
    for name in axis_names:
      if 'kappa' in name.lower():
        kappa_name = name
      elif 'chi' in name.lower():
        chi_name = name
      elif 'phi' in name.lower():
        phi_name = name
    assert [chi_name, kappa_name].count(None) == 1
    assert phi_name is not None

    for solution_id, soln in enumerate(solutions):
      if chi_name is not None:
        chi = soln.get(chi_name)
      else: chi = None
      if kappa_name is not None:
        kappa = soln.get(kappa_name)
      else: kappa = None
      phi = soln.get(phi_name)
      primary_axis = soln.get('primary_axis')[0]
      primary_axis_type = soln.get('primary_axis_type')[0]
      assert [chi, kappa].count(None) == 1
      assert phi is not None
      settings_str = '%s' %primary_axis
      if primary_axis_type is not None:
        settings_str = '%s (%i-fold)' %(settings_str, int(primary_axis_type))
      self.send_alignment_result_to_ispyb(
        dcid,
        crystal_symmetry,
        'dials.align_crystal',
        settings_str,
        'dials.align_crystal %i' %solution_id,
        chi=chi,
        kappa=kappa,
        phi=phi
      )

  def send_alignment_result_to_ispyb(self,
      dcid, crystal_symmetry, program, comments, short_comments,
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

    uc_params = crystal_symmetry.unit_cell().parameters()
    for i, p in enumerate(('a','b','c','alpha','beta','gamma')):
      result['unitcell%s' % p] = uc_params[i]
    result['spacegroup'] = crystal_symmetry.space_group_info().type().lookup_symbol()

    logger.info('Inserting alignment result into ISPyB: %s' %str(result))
    self.recwrap.send_to('alignment-result', result)

  def construct_commandline(self, params):
    '''Construct dlstbx.align_crystal command line.
       Takes job parameter dictionary, returns array.'''

    template = params['image_template']
    pattern = params['image_pattern']
    first = int(params['image_first'])
    last = int(params['image_last'])
    image_files = [
      os.path.join(params['image_directory'], pattern % i)
      for i in range(first, last+1)
    ]

    command = ['dlstbx.align_crystal'] + image_files

    return command

  def hdf5_to_cbf(self):
    params = self.recwrap.recipe_step['job_parameters']
    working_directory = py.path.local(params['working_directory'])
    tmpdir = working_directory.join('image-tmp')
    tmpdir.ensure(dir=True)
    master_h5 = os.path.join(params['image_directory'], params['image_template'])
    prefix = params['image_template'].split('master.h5')[0]
    params['image_pattern'] = prefix + '%04d.cbf'
    params['image_template'] = prefix + '####.cbf'
    logger.info('Image pattern: %s', params['image_pattern'])
    logger.info('Image template: %s', params['image_template'])
    logger.info(
      'Converting %s to %s' % (master_h5, tmpdir.join(params['image_pattern'])))
    result = procrunner.run_process(
      ['dlstbx.snowflake2cbf', master_h5, params['image_pattern']],
      working_directory=tmpdir.strpath,
      timeout=params.get('timeout', 3600),
    )
    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    params['orig_image_directory'] = params['image_directory']
    params['image_directory'] = tmpdir.strpath

  def run(self):
    assert hasattr(self, 'recwrap'), "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']

    if params['image_template'].endswith('.h5'):
      self.hdf5_to_cbf()

    command = self.construct_commandline(params)

    working_directory = py.path.local(params['working_directory'])
    results_directory = py.path.local(params['results_directory'])

    # Create working directory with symbolic link
    working_directory.ensure(dir=True)

    # run dlstbx.align_crystal in working directory
    result = procrunner.run(
        command, timeout=params.get('timeout'),
        #print_stdout=False, print_stderr=False,
        working_directory=working_directory.strpath,
    )

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    # Create results directory and symlink if they don't already exist
    results_directory.ensure(dir=True)

    # copy output files to result directory and attach them in ISPyB
    keep_ext = {
      #'.json': 'result',
      '.log': 'log',
    }
    keep = {
      'align_crystal.json': 'result',
      'bravais_summary.json': 'result',
    }
    allfiles = []
    for filename in working_directory.listdir():
      filetype = keep_ext.get(filename.ext)
      if filename.basename in keep:
        filetype = keep[filename.basename]
      if filetype is None:
        continue
      destination = results_directory.join(filename.basename)
      logger.debug('Copying %s to %s' % (filename.strpath, destination.strpath))
      allfiles.append(destination.strpath)
      filename.copy(destination)
      if filetype:
        self.record_result_individual_file({
          'file_path': destination.dirname,
          'file_name': destination.basename,
          'file_type': filetype,
        })
    if allfiles:
      self.record_result_all_files({ 'filelist': allfiles })

    assert working_directory.join('reindexed_experiments.json')
    experiments = load.experiment_list(
      working_directory.join('reindexed_experiments.json').strpath)
    crystal_symmetry = experiments[0].crystal.get_crystal_symmetry()
    # Forward JSON results if possible
    if working_directory.join('align_crystal.json').check():
      with working_directory.join('align_crystal.json').open('rb') as fh:
        json_data = json.load(fh)
        self.insert_dials_align_strategies(
          params['dcid'], crystal_symmetry, json_data)
    elif result['exitcode']:
      logger.info('dlstbx.align_crystal failed to process the dataset')
    else:
      logger.warning('Expected JSON output file missing')

    return result['exitcode'] == 0
