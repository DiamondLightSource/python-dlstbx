from __future__ import absolute_import, division, print_function

import logging
import os

import dlstbx.zocalo.wrapper
import procrunner

logger = logging.getLogger('dlstbx.wrap.snmct')

class SNMCTWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def construct_commandline(self, params):
    '''Construct snmct command line.
       Takes job parameter dictionary, returns array.'''

    command = ['xia2.multi_crystal_scale']

    appids = params['appids']
    if not appids:
      dcids = self.get_dcids(params)
      appids = self.get_appids(dcids)
    data_files = self.get_data_files_for_appids(appids)
    for f in data_files:
      command.append(f)

    return command

  def get_data_files_for_appids(self, appids):
    data_files = []
    for appid in appids:
      attachments = self.ispyb_conn.mx_processing.retrieve_program_attachments_for_program_id(appid)
      for item in attachments:
        if item['fileType'] == 'Result':
          if (item['fileName'].endswith('experiments.json') or
              item['fileName'].endswith('reflections.pickle')):
            data_files.append(os.path.join(item['filePath'], item['fileName']))
    return data_files

  def get_appids(self, dcids):
    appids = []
    for dcid in dcids:
      dc = self.ispyb_conn.get_data_collection(dcid)
      intgr = dc.integrations
      for intgr in dc.integrations:
        appids.append(intgr.APPID)
    return appids

  def get_dcids(self, params):
    this_dcid = params['dcid']
    dcids = params['dcids']
    if not dcids:
      command = [
        '/dls_sw/apps/mx-scripts/misc/GetAListOfAssociatedDCOnThisCrystalOrDir.sh',
        this_dcid
      ]
      result = procrunner.run_process(
        command, timeout=params.get('timeout'),
        working_directory=params['working_directory'],
        print_stdout=False, print_stderr=False)
      dcids = result['stdout'].split()
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

    working_directory = params['working_directory']
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)

    command = self.construct_commandline(params)

    # run SNMCT in working directory

    result = procrunner.run_process(
      command, timeout=params.get('timeout'),
      working_directory=params['working_directory'],
      print_stdout=False, print_stderr=False)

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    return result['exitcode'] == 0
