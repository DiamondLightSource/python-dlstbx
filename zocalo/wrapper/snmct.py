from __future__ import absolute_import, division, print_function

import logging
import py

import dlstbx.util.symlink
import dlstbx.zocalo.wrapper
import procrunner

logger = logging.getLogger('dlstbx.wrap.snmct')

class SNMCTWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def construct_commandline(self, params):
    '''Construct snmct command line.
       Takes job parameter dictionary, returns array.'''

    command = ['xia2.multi_crystal_scale']

    appids = params['appids']
    data_files = [self.get_data_files_for_appid(appid) for appid in appids if appid is not None]
    for files in data_files:
      for f in files:
        command.append(f.strpath)

    return command

  def get_data_files_for_appid(self, appid):
    data_files = []
    logger.info('Retrieving program attachment for appid %s', appid)
    attachments = self.ispyb_conn.mx_processing.retrieve_program_attachments_for_program_id(appid)
    for item in attachments:
      if item['fileType'] == 'Result':
        if (item['fileName'].endswith('experiments.json') or
            item['fileName'].endswith('reflections.pickle')):
          data_files.append(py.path.local(item['filePath']).join(item['fileName']))
    logger.info('Found the following files for appid %s:', appid)
    logger.info(list(data_files))
    assert len(data_files) == 2, data_files
    return data_files

  def get_appid(self, dcid):
    appid = {}
    dc = self.ispyb_conn.get_data_collection(dcid)
    for intgr in dc.integrations:
      prg = intgr.program
      if ((prg.message != 'processing successful') or
          (prg.name != 'xia2 dials')):
        continue
      appid[prg.time_update] = intgr.APPID
    if not appid:
      return None
    return appid.values()[0]

  def get_dcids(self, params):
    this_dcid = int(params['dcid'])
    dcids = params['dcids']
    if not dcids:
      command = [
        '/dls_sw/apps/mx-scripts/misc/GetAListOfAssociatedDCOnThisCrystalOrDir.sh',
        '%i' % this_dcid
      ]
      result = procrunner.run_process(
        command, timeout=params.get('timeout'),
        working_directory=params['working_directory'],
        print_stdout=False, print_stderr=False)
      dcids = [int(dcid) for dcid in result['stdout'].split()]
      dcids = [dcid for dcid in dcids if dcid < this_dcid]
    return [this_dcid] + dcids

  def send_resuls_to_ispyb(self, json_file):
    from dlstbx.ispybtbx import ispybtbx
    ispyb_conn = ispybtbx()
    return

  def run(self):
    import ispyb
    import ispyb.model.__future__
    ispyb.model.__future__.enable('/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
    self.ispyb_conn = ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')

    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']

    if not params['appids']:
      dcids = self.get_dcids(params)
      if len(dcids) == 1:
        logger.info('Not running SNMCT: no related dcids for dcid %s' % dcids[0])
        return
      appids = [self.get_appid(dcid) for dcid in dcids]
      params['appids'] = appids
      logger.info('Found dcids: %s', str(dcids))
      logger.info('Found appids: %s', str(appids))

    # Adjust all paths if a spacegroup is set in ISPyB
    if params.get('ispyb_parameters'):
      if params['ispyb_parameters'].get('spacegroup') and \
          '/' not in params['ispyb_parameters']['spacegroup']:
        for parameter in ('working_directory', 'results_directory', 'create_symlink'):
          if parameter in params:
            params[parameter] += '-' + params['ispyb_parameters']['spacegroup']

    command = self.construct_commandline(params)

    working_directory = py.path.local(params['working_directory'])
    results_directory = py.path.local(params['results_directory'])

    # Create working directory with symbolic link
    working_directory.ensure(dir=True)
    if params.get('create_symlink'):
      dlstbx.util.symlink.create_parent_symlink(working_directory.strpath, params['create_symlink'])

    # run SNMCT in working directory

    result = procrunner.run_process(
      command, timeout=params.get('timeout'),
      working_directory=working_directory.strpath,
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
    results_directory.ensure(dir=True)
    if params.get('create_symlink'):
      dlstbx.util.symlink.create_parent_symlink(results_directory.strpath, params['create_symlink'])

    keep_ext = {
      '.png': None,
      '.log': 'log',
      '.json': None,
      '.pickle': None,
      '.mtz': None,
      '.html': 'log',
    }
    keep = {
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
    #if allfiles:
      #self.record_result_all_files({ 'filelist': allfiles })

    return result['exitcode'] == 0
