from __future__ import absolute_import, division, print_function

import logging
import os

import dlstbx.zocalo.wrapper
import procrunner
import shutil

logger = logging.getLogger('dlstbx.wrap.dimple')

class DimpleWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def construct_commandline(self):
    '''Construct dimple command line.
       Takes job parameter dictionary, returns array.'''

    command = ['dimple']

    mtz = os.path.abspath(self._params['dimple']['data'])
    pdb = self.get_matching_pdb()
    if not len(pdb):
      logger.info('Not running dimple as no PDB file available')
      return
    assert os.path.exists(mtz), mtz
    command.append(mtz)
    for p in pdb:
      command.append(p)
    output_dir = os.path.abspath(self._params['working_directory'])
    command.append(output_dir)
    #command.append('--dls-naming')
    command.append('-fpng')
    return command

  def get_matching_pdb(self):
    import ispyb.model.__future__
    i = ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')
    ispyb.model.__future__.enable('/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
    dcid = self._params['dcid']
    working_directory = os.path.abspath(self._params['working_directory'])
    pdb = i.get_data_collection(dcid).pdb
    results = []
    for pdb in i.get_data_collection(dcid).pdb:
      #logger.info(pdb.name, pdb.code, pdb.rawfile)
      if pdb.code is not None:
        results.append(pdb.code)
      elif pdb.rawfile is not None:
        assert pdb.name is not None
        pdb_filepath = os.path.join(working_directory, '%s.pdb' % pdb.name)
        with open(pdb_filepath, 'wb') as f:
          f.write(pdb.rawfile)
        results.append(pdb_filepath)
    return results

  def send_results_to_ispyb(self, working_directory, fast_dp_directory):
    logger.debug('Inserting dimple phasing results into ISPyB')

    ispyb_config_file = os.environ.get('ISPYB_CONFIG_FILE')
    assert ispyb_config_file is not None
    command = [
      'dimple2ispyb.py',
      ispyb_config_file,
      working_directory,
      fast_dp_directory
    ]
    result = procrunner.run_process(
      command,
      working_directory=working_directory,
      print_stdout=True, print_stderr=True)

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    self._params = self.recwrap.recipe_step['job_parameters']

    command = self.construct_commandline()
    if command is None:
      return

    working_directory = os.path.abspath(self._params['working_directory'])
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)

    result = procrunner.run_process(
      command,
      working_directory=working_directory,
      timeout=self._params.get('timeout'),
      print_stdout=True, print_stderr=True)

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    logger.info('Sending dimple results to ISPyB')
    self.send_results_to_ispyb(
      working_directory, os.path.dirname(
        os.path.abspath(self._params['dimple']['data'])))

    # copy output files to result directory
    results_directory = os.path.abspath(self._params['results_directory'])
    if not os.path.exists(results_directory):
      logger.info('Copying results to %s', results_directory)
      shutil.copytree(working_directory, results_directory)
    else:
      logger.info('Results directory already exists: %s', results_directory)

    return result['exitcode'] == 0
