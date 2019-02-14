from __future__ import absolute_import, division, print_function

import logging
import os
import shutil

import procrunner
import zocalo.wrapper

logger = logging.getLogger('dlstbx.wrap.anode')

class AnodeWrapper(zocalo.wrapper.BaseWrapper):

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    self._params = self.recwrap.recipe_step['job_parameters']

    working_directory = os.path.abspath(self._params['working_directory'])
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)

    pdb_file = str(os.path.abspath(self._params['anode']['model']))
    if not os.path.exists(pdb_file):
      logger.info(
        'Not running anode as required input pdb file %s does not exist' % pdb_file)
      return

    shutil.copyfile(
      pdb_file, os.path.join(working_directory, 'anode_input.pdb'))

    mtz_file = str(os.path.abspath(self._params['anode']['data']))
    assert os.path.exists(mtz_file), mtz_file
    sca_file = str(os.path.join(working_directory, 'anode_input.sca'))

    self.run_mtz2sca(mtz_file, sca_file, working_directory)
    self.run_shelxc(sca_file, working_directory)
    result = self.run_anode(working_directory)
    return result['exitcode'] == 0

  def run_mtz2sca(self, mtz_file, sca_file, working_directory):
    result = procrunner.run(
      ['mtz2sca', mtz_file, sca_file],
      working_directory=working_directory,
      timeout=self._params.get('timeout'),
    )

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])
    assert os.path.exists(sca_file), sca_file
    return result

  def run_shelxc(self, sca_file, working_directory):
    # run shelxc to prepare required input anode

    from iotbx.reflection_file_reader import any_reflection_file
    reader = any_reflection_file(sca_file)
    arrays = reader.as_miller_arrays()
    crystal_symmetry = arrays[0].crystal_symmetry()
    cell_str = ' '.join(['%.4f']*6) % crystal_symmetry.unit_cell().parameters()
    sg_str = crystal_symmetry.space_group().type().lookup_symbol().replace(' ', '')
    stdin = '\n'.join([
      'SAD %s' % sca_file,
      'CELL %s' % cell_str,
      'SPAG %s' % sg_str,
      'MAXM 2'
    ])

    with open(os.path.join(working_directory, 'shelxc.log'), 'wb') as f:
      result = procrunner.run(
        ['shelxc', 'anode_input'],
        stdin=stdin,
        callback_stdout=lambda x: print(x, file=f),
        working_directory=working_directory,
        timeout=self._params.get('timeout'),
      )

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])
    return result

  def run_anode(self, working_directory):
    with open(os.path.join(working_directory, 'anode.log'), 'wb') as f:
      result = procrunner.run(
        ['anode', 'anode_input'],
        callback_stdout=lambda x: print(x, file=f),
        working_directory=working_directory,
        timeout=self._params.get('timeout'),
      )

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])
    return result
