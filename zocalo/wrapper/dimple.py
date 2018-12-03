from __future__ import absolute_import, division, print_function

import ConfigParser
from datetime import datetime
import logging
import os
import shutil
import sys

import dlstbx.util.symlink
import dlstbx.zocalo.wrapper
import ispyb
import ispyb.model.__future__
import procrunner
import py

logger = logging.getLogger('dlstbx.wrap.dimple')

class DimpleWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def get_matching_pdb(self):
    results = []
    with ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg') as i:
      ispyb.model.__future__.enable('/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
      dcid = self.params['dcid']
      for pdb in i.get_data_collection(dcid).pdb:
        #logger.info(pdb.name, pdb.code, pdb.rawfile)
        if pdb.code is not None:
          results.append(pdb.code)
        elif pdb.rawfile is not None:
          assert pdb.name and '/' not in pdb.name, 'Invalid PDB file name'
          pdb_filepath = self.working_directory.join('%s.pdb' % pdb.name)
          pdb_filepath.write(pdb.rawfile, ensure=True)
          results.append(pdb_filepath.strpath)
    return results

  def send_results_to_ispyb(self):
    log_file = self.results_directory.join('dimple.log')
    if not log_file.check():
      logger.error('Can not insert dimple results into ISPyB: dimple.log not found')
      return False
    log = ConfigParser.RawConfigParser()
    log.read(log_file.strpath)

    scaling_id = self.params.get('scaling_id')
    if not str(scaling_id).isdigit():
      scaling_id = self.params.get('ispyb_parameters', {}).get('scaling_id')
      if not str(scaling_id).isdigit():
        logger.error('Can not write results to ISPyB: no scaling ID set (%r)', scaling_id)
        return False
    scaling_id = int(scaling_id)
    logger.debug('Inserting dimple phasing results from %s into ISPyB for scaling_id %d',
        self.results_directory.strpath, scaling_id)

    # see also /dls_sw/apps/python/anaconda/1.7.0/64/bin/dimple2ispyb.py
    with ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg') as conn:
      params = conn.mx_processing.get_run_params()
      params['parentid'] = scaling_id
      params['pipeline'] = 'dimple'
      params['log_file'] = log_file.strpath
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
      params['run_dir'] = self.results_directory.strpath
      dimple_args = log.get('workflow', 'args').split()
      params['input_MTZ_file'] = dimple_args[0]
      params['input_coord_file'] = dimple_args[1]
      params['output_MTZ_file'] = self.results_directory.join('final.mtz').strpath
      params['output_coord_file'] = self.results_directory.join('final.pdb').strpath
      params['cmd_line'] = log.get('workflow', 'prog') + ' ' + log.get('workflow', 'args').replace('\n', ' ')
      mr_id = conn.mx_processing.upsert_run(list(params.values()))

      for n in (1,2):
        if self.results_directory.join('/blob{}v1.png'.format(n)).check():
          blobparam = conn.mx_processing.get_run_blob_params()
          blobparam['parentid'] = mr_id
          blobparam['view1'] = 'blob{0}v1.png'.format(n)
          blobparam['view2'] = 'blob{0}v2.png'.format(n)
          blobparam['view3'] = 'blob{0}v3.png'.format(n)
          mrblob_id = conn.mx_processing.upsert_run_blob(list(blobparam.values()))
    return True

  def run(self):
    assert hasattr(self, 'recwrap'), "No recipewrapper object found"
    self.params = self.recwrap.recipe_step['job_parameters']
    self.working_directory = py.path.local(self.params['working_directory'])
    self.results_directory = py.path.local(self.params['results_directory'])

    mtz = self.params.get('ispyb_parameters', {}).get('data') \
        or self.params['dimple']['data']
    if not mtz:
      logger.error('Could not identify on what data to run')
      return False
    mtz = os.path.abspath(mtz)
    if not os.path.exists(mtz):
      logger.error('Could not find data file to process')
      return False
    pdb = self.get_matching_pdb()
    if not pdb:
      logger.error('Not running dimple as no PDB file available')
      return False

    command = ['dimple', mtz] \
        + pdb \
        + [
            self.working_directory.strpath,
            # '--dls-naming',
            '-fpng',
          ]

    self.working_directory.ensure(dir=True)
    if self.params.get('create_symlink'):
      dlstbx.util.symlink.create_parent_symlink(self.working_directory.strpath, self.params['create_symlink'])

    # Create SynchWeb ticks hack file. This will be deleted or replaced later.
    # For this we need to create the results directory and its symlink immediately.
    if self.params.get('synchweb_ticks') and self.params.get('ispyb_parameters', {}).get('set_synchweb_status'):
      logger.debug('Setting SynchWeb status to swirl')
      if self.params.get('create_symlink'):
        self.results_directory.ensure(dir=True)
        dlstbx.util.symlink.create_parent_symlink(self.results_directory.strpath, self.params['create_symlink'])
        mtzsymlink = os.path.join(os.path.dirname(mtz), self.params['create_symlink'])
        if not os.path.exists(mtzsymlink):
          deltapath = os.path.relpath(self.results_directory.strpath, os.path.dirname(mtz))
          os.symlink(deltapath, mtzsymlink)
      py.path.local(self.params['synchweb_ticks']).ensure()

    result = procrunner.run(
        command,
        working_directory=self.working_directory.strpath,
        timeout=self.params.get('timeout'),
        print_stdout=True, print_stderr=True,
    )

    # Hack to workaround dimple returning successful exitcode despite 'Giving up'
    if 'Giving up' in result['stdout']:
      result['exitcode'] = 1

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    logger.info('Copying DIMPLE results to %s', self.results_directory.strpath)
    self.results_directory.ensure(dir=True)
    if self.params.get('create_symlink'):
      dlstbx.util.symlink.create_parent_symlink(self.results_directory.strpath, self.params['create_symlink'])
      mtzsymlink = os.path.join(os.path.dirname(mtz), self.params['create_symlink'])
      if not os.path.exists(mtzsymlink):
        deltapath = os.path.relpath(self.results_directory.strpath, os.path.dirname(mtz))
        os.symlink(deltapath, mtzsymlink)
    for f in self.working_directory.listdir():
      if f.basename.startswith('.'): continue
      if any(f.ext == skipext for skipext in ('.pickle', '.py', '.r3d', '.sh')):
        continue
      f.copy(self.results_directory)

    if result['exitcode'] == 0:
      logger.info('Sending dimple results to ISPyB')
      success = self.send_results_to_ispyb()
    else:
      logger.warning('dimple failed: %s/dimple.log' % self.working_directory)
      success = False

    # Update SynchWeb tick hack file
    if self.params.get('synchweb_ticks') and self.params.get('ispyb_parameters', {}).get('set_synchweb_status'):
      if success:
        logger.debug('Removing SynchWeb hack file')
        py.path.local(self.params['synchweb_ticks']).remove()
      else:
        logger.debug('Updating SynchWeb hack file to failure')
        py.path.local(self.params['synchweb_ticks']).write(
            'This file is used as a flag to synchweb to show the processing has failed'
        )

    return success
