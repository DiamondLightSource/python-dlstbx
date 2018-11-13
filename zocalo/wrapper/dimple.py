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

    mtz = None
    if self._params.get('ispyb_parameters'):
      if self._params['ispyb_parameters'].get('data'):
        mtz = os.path.abspath(self._params['ispyb_parameters'].get('data'))
    if mtz is None:
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
    results = []
    if self._params.get('ispyb_parameters'):
      if self._params['ispyb_parameters'].get('pdb'):
        results = self._params['ispyb_parameters'].get('pdb')
        if isinstance(results, basestring):
          results = [results]
    if not results:
      import ispyb.model.__future__
      i = ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')
      ispyb.model.__future__.enable('/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
      dcid = self._params['dcid']
      working_directory = os.path.abspath(self._params['working_directory'])
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

  def get_scaling_id(self):
    '''Read the fast_dp output file, extract and return the autoProcScalingId'''
    if self._params.get('ispyb_parameters'):
      # this should eventually be the main way of getting scaling_id
      if self._params['ispyb_parameters'].get('scaling_id'):
        return self._params['ispyb_parameters'].get('scaling_id')
    ispyb_ids_xml = os.path.join(
      os.path.dirname(os.path.abspath(self._params['dimple']['data'])),
      'ispyb_ids.xml')
    import re
    if (not os.path.isfile(ispyb_ids_xml)) or (not os.access(ispyb_ids_xml, os.R_OK)):
      logger.warn("Either file %s is missing or is not readable" % ispyb_ids_xml)
      return
    xml = None
    with open(ispyb_ids_xml, 'rb') as f:
      xml = f.read()
    if xml is None:
      return

    m = re.match(r'.*\<autoProcScalingId\>(\d+)\<\/autoProcScalingId\>.*', xml)
    if m is None:
      return
    else:
      return m.group(1)

  def send_results_to_ispyb(self):
    logger.debug('Inserting dimple phasing results into ISPyB')

    working_directory = os.path.abspath(self._params['working_directory'])
    log_file = os.path.join(working_directory, 'dimple.log')
    scaling_id = self.get_scaling_id()

    import ispyb
    import ConfigParser
    from datetime import datetime
    # see also /dls_sw/apps/python/anaconda/1.7.0/64/bin/dimple2ispyb.py
    ispyb_config_file = os.environ.get('ISPYB_CONFIG_FILE')
    with ispyb.open(ispyb_config_file) as conn:
      if (not os.path.isfile(log_file)) or (not os.access(log_file, os.R_OK)):
          logger.warn("Either file %s is missing or is not readable" % log_file)
          return

      log = ConfigParser.RawConfigParser()
      log.read(log_file)

      params = conn.mx_processing.get_run_params()
      params['parentid'] = scaling_id
      params['pipeline'] = 'dimple'
      params['log_file'] = log_file
      params['success'] = 1

      starttime = log.get(log.sections()[1], 'start_time')
      params['starttime'] = datetime.strptime(starttime, '%Y-%m-%d %H:%M:%S')
      endtime = log.get(log.sections()[-1], 'end_time')
      params['endtime'] = datetime.strptime(endtime, '%Y-%m-%d %H:%M:%S')

      params['rfree_start'] = log.getfloat('refmac5 restr', 'ini_free_r')
      params['rfree_end'] = log.getfloat('refmac5 restr', 'free_r')

      params['r_start'] = log.getfloat('refmac5 restr', 'ini_overall_r')
      params['r_end'] = log.getfloat('refmac5 restr', 'overall_r')
      params['message'] = " ".join(log.get('find-blobs', 'info').split()[:4])
      params['run_dir'] = working_directory
      dimple_args = log.get('workflow', 'args').split()
      params['input_MTZ_file'] = dimple_args[0]
      params['input_coord_file'] = dimple_args[1]
      params['output_MTZ_file'] = working_directory + '/final.mtz'
      params['output_coord_file'] = working_directory + '/final.pdb'
      params['cmd_line'] = log.get('workflow', 'prog') + ' ' + log.get('workflow', 'args').replace('\n', ' ')
      mr_id = conn.mx_processing.upsert_run(list(params.values()))

      for n in (1,2):
        if os.path.exists(working_directory+'/blob{0}v1.png'.format(n)):
          blobparam = conn.mx_processing.get_run_blob_params()
          blobparam['parentid'] = mr_id
          blobparam['view1'] = 'blob{0}v1.png'.format(n)
          blobparam['view2'] = 'blob{0}v2.png'.format(n)
          blobparam['view3'] = 'blob{0}v3.png'.format(n)
          mrblob_id = conn.mx_processing.upsert_run_blob(list(blobparam.values()))

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
    self.send_results_to_ispyb()

    # copy output files to result directory
    results_directory = None
    if self._params.get('ispyb_parameters'):
      # this should eventually be the main way of getting scaling_id
      if self._params['ispyb_parameters'].get('results_directory'):
        results_directory = self._params['ispyb_parameters'].get('results_directory')
    if results_directory is None:
      results_directory = os.path.abspath(self._params['results_directory'])
    if not os.path.exists(results_directory):
      logger.info('Copying results to %s', results_directory)
      shutil.copytree(working_directory, results_directory)
    else:
      logger.info('Results directory already exists: %s', results_directory)

    return result['exitcode'] == 0
