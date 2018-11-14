from __future__ import absolute_import, division, print_function

import json
import logging
import os
import shutil

import dlstbx.util.symlink
import dlstbx.zocalo.wrapper
import procrunner
import py

logger = logging.getLogger('dlstbx.wrap.fast_dp')

class FastDPWrapper(dlstbx.zocalo.wrapper.BaseWrapper):
  @staticmethod
  def xml_to_dict(filename):
    def make_dict_from_tree(element_tree):
        """Traverse the given XML element tree to convert it into a dictionary.

        :param element_tree: An XML element tree
        :type element_tree: xml.etree.ElementTree
        :rtype: dict
        """
        def internal_iter(tree, accum):
            """Recursively iterate through the elements of the tree accumulating
            a dictionary result.

            :param tree: The XML element tree
            :type tree: xml.etree.ElementTree
            :param accum: Dictionary into which data is accumulated
            :type accum: dict
            :rtype: dict
            """
            if tree is None:
                return accum
            if tree.getchildren():
                accum[tree.tag] = {}
                for each in tree.getchildren():
                    result = internal_iter(each, {})
                    if each.tag in accum[tree.tag]:
                        if not isinstance(accum[tree.tag][each.tag], list):
                            accum[tree.tag][each.tag] = [
                                accum[tree.tag][each.tag]
                            ]
                        accum[tree.tag][each.tag].append(result[each.tag])
                    else:
                        accum[tree.tag].update(result)
            else:
                accum[tree.tag] = tree.text
            return accum
        return internal_iter(element_tree, {})
    import xml.etree.ElementTree
    return make_dict_from_tree(xml.etree.ElementTree.parse(filename).getroot())

  def send_results_to_ispyb(self, xml_file):
    logger.debug("Reading fast_dp results")
    message = self.xml_to_dict(xml_file)['AutoProcContainer']
    # Do not accept log entries from the object, we add those separately
    message['AutoProcProgramContainer']['AutoProcProgramAttachment'] = filter(
       lambda x: x.get('fileType') != 'Log', message['AutoProcProgramContainer']['AutoProcProgramAttachment'])

    def recursive_replace(thing, old, new):
      '''Recursive string replacement in data structures.'''

      def _recursive_apply(item):
        '''Internal recursive helper function.'''
        if isinstance(item, basestring):
          return item.replace(old, new)
        if isinstance(item, dict):
          return { _recursive_apply(key): _recursive_apply(value) for
                   key, value in item.items() }
        if isinstance(item, tuple):
          return tuple(_recursive_apply(list(item)))
        if isinstance(item, list):
          return [ _recursive_apply(x) for x in item ]
        return item
      return _recursive_apply(thing)

    logger.debug("Replacing temporary zocalo paths with correct destination paths")
    message = recursive_replace(
        message,
        self.recwrap.recipe_step['job_parameters']['working_directory'],
        self.recwrap.recipe_step['job_parameters']['results_directory']
      )

    dcid = int(self.recwrap.recipe_step['job_parameters']['dcid'])
    assert dcid > 0, "Invalid data collection ID given."
    logger.debug("Writing to data collection ID %s", str(dcid))
    if isinstance(message['AutoProcScalingContainer']['AutoProcIntegrationContainer'], dict):  # Make it a list regardless
      message['AutoProcScalingContainer']['AutoProcIntegrationContainer'] = [message['AutoProcScalingContainer']['AutoProcIntegrationContainer']]
    for container in message['AutoProcScalingContainer']['AutoProcIntegrationContainer']:
      container['AutoProcIntegration']['dataCollectionId'] = dcid

    ## Use existing AutoProcProgramID
    #if self.recwrap.environment.get('ispyb_autoprocprogram_id'):
    #  message['AutoProcProgramContainer']['AutoProcProgram'] = \
    #    self.recwrap.environment['ispyb_autoprocprogram_id']

    logger.debug("Sending %s", str(message))
    #self.recwrap.transport.send('ispyb', message)

    import ispyb
    from ispyb.xmltools import mx_data_reduction_to_ispyb
    # see also /dls_sw/apps/python/anaconda/1.7.0/64/bin/mxdatareduction2ispyb.py
    ispyb_config_file = os.environ.get('ISPYB_CONFIG_FILE')
    with ispyb.open(ispyb_config_file) as conn:
      (app_id, ap_id, scaling_id, integration_id) = mx_data_reduction_to_ispyb(
        message, dcid, conn.mx_processing)

    # Write results to xml_out_file
    ispyb_ids_xml = os.path.join(
      self.recwrap.recipe_step['job_parameters']['working_directory'],
      'ispyb_ids.xml')
    with open(ispyb_ids_xml, 'wb') as f:
      f.write(
        '<?xml version="1.0" encoding="ISO-8859-1"?>'\
        '<dbstatus><autoProcProgramId>%d</autoProcProgramId>'\
        '<autoProcId>%d</autoProcId>'\
        '<autoProcScalingId>%d</autoProcScalingId>'\
        '<autoProcIntegrationId>%d</autoProcIntegrationId>'\
        '<code>ok</code></dbstatus>' % (app_id, ap_id, scaling_id, integration_id))

    self._scaling_id = scaling_id
    logger.info("Saved fast_dp information for data collection %s", str(dcid))

  def run_dimple(self, scaling_id):
    params = self.recwrap.recipe_step['job_parameters']

    def has_matching_pdb(dcid):
      import ispyb.model.__future__
      i = ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')
      ispyb.model.__future__.enable('/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
      for pdb in i.get_data_collection(dcid).pdb:
        if pdb.code is not None:
          return True
        elif pdb.rawfile is not None:
          assert pdb.name is not None
          return True
      return False

    if has_matching_pdb(params['dcid']):
      results_directory = os.path.abspath(params['results_directory'])
      fast_dp_mtz = os.path.join(results_directory, 'fast_dp.mtz')
      command = [
        'ispyb.job', '--new', '--dcid',
         '%s' % params['dcid'],
         '--trigger',
         '--recipe', 'postprocessing-dimple',
         '--add-param=data:%s' % fast_dp_mtz,
         '--add-param=results_directory:%s/dimple' % results_directory,
         '--add-param=scaling_id:%s' % scaling_id,
         '-v'
      ]
      # run ispyb.job to launch new dimple zocalo job
      result = procrunner.run_process(
        command, timeout=params.get('timeout'),
        print_stdout=True, print_stderr=True,
        working_directory=params['working_directory'])

  def run_fast_ep(self, scaling_id):
    params = self.recwrap.recipe_step['job_parameters']

    results_directory = os.path.abspath(params['results_directory'])
    fast_dp_mtz = os.path.join(results_directory, 'fast_dp.mtz')
    command = [
      'ispyb.job', '--new', '--dcid',
       '%s' % params['dcid'],
       '--trigger',
       '--recipe', 'postprocessing-fast-ep',
       '--add-param=data:%s' % fast_dp_mtz,
       '--add-param=check_go_fast_ep:True',
       '--add-param=results_directory:%s/dimple' % results_directory,
       '--add-param=scaling_id:%s' % scaling_id,
       '-v'
    ]
    # run ispyb.job to launch new fast_ep zocalo job
    result = procrunner.run_process(
      command, timeout=params.get('timeout'),
      print_stdout=True, print_stderr=True,
      working_directory=params['working_directory'])

  def construct_commandline(self, params):
    '''Construct fast_dp command line.
       Takes job parameter dictionary, returns array.'''

    command = ['fast_dp', '--atom=S', '-l', 'durin-plugin.so', params['fast_dp']['filename']]

    if params.get('ispyb_parameters'):
      if params['ispyb_parameters'].get('d_min'):
        command.append('--resolution-low=%s' % params['ispyb_parameters']['d_min'])
      if params['ispyb_parameters'].get('spacegroup'):
        command.append('--spacegroup=%s' % params['ispyb_parameters']['spacegroup'])
      if params['ispyb_parameters'].get('unit_cell'):
        command.append('--cell=%s' % params['ispyb_parameters']['unit_cell'])

    return command

  def run(self):
    assert hasattr(self, 'recwrap'), "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    command = self.construct_commandline(params)

    working_directory = py.path.local(params['working_directory'])
    results_directory = py.path.local(params['results_directory'])

    # Create working directory with symbolic link
    working_directory.ensure(dir=True)
    if params.get('results_symlink'):
      dlstbx.util.symlink.create_parent_symlink(working_directory.strpath, params['results_symlink'])

    # Create SynchWeb ticks hack file. This will be overwritten with the real log later.
    # For this we need to create the results directory and symlink immediately.
    if params.get('synchweb_ticks'):
      logger.debug('Setting SynchWeb status to swirl')
      if params.get('results_symlink'):
        results_directory.ensure(dir=True)
        dlstbx.util.symlink.create_parent_symlink(results_directory.strpath, params['results_symlink'])
      py.path.local(params['synchweb_ticks']).ensure()

    # run fast_dp in working directory
    result = procrunner.run_process(
      command, timeout=params.get('timeout'),
      print_stdout=False, print_stderr=False,
      working_directory=working_directory.strpath)

    if working_directory.join('fast_dp.error').check():
      # fast_dp anomaly: exit code 0 and no stderr output still means failure if error file exists
      result['exitcode'] = 1

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    if result['exitcode'] == 0:
      command = [
          'xia2.report',
          'log_include=%s' % working_directory.join('fast_dp.log').strpath,
          'prefix=fast_dp',
          'title=fast_dp',
          'fast_dp_unmerged.mtz'
      ]
      # run fast_dp in working directory
      result = procrunner.run_process(
          command, timeout=params.get('timeout'),
          print_stdout=False, print_stderr=False,
          working_directory=working_directory.strpath)

    # Create results directory and symlink if they don't already exist
    results_directory.ensure(dir=True)
    if params.get('results_symlink'):
      dlstbx.util.symlink.create_parent_symlink(results_directory.strpath, params['results_symlink'])

    # copy output files to result directory
    keep_ext = {
      ".INP": False,
      ".xml": False,
      ".log": 'log',
      ".html": 'log',
      ".txt": "log",
      ".error": "log",
      ".LP": "log",
      ".HKL": "result",
      ".sca": "result",
      ".mtz": "result",
    }
    keep = {
      "fast_dp-report.json": "graph",
      "iotbx-merging-stats.json": "graph",
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

# Correct way:
#    # Forward JSON results if possible
#    if os.path.exists('fast_dp.json'):
#      with open('fast_dp.json', 'rb') as fh:
#        json_data = json.load(fh)
#      self.recwrap.send_to('result-json', json_data)
#    else:
#      logger.warning('Expected JSON output file missing')

# Wrong way:
    xml_file = os.path.join(working_directory, 'fast_dp.xml')
    if os.path.exists(xml_file):
      self.send_results_to_ispyb(xml_file)
      self.run_dimple(self._scaling_id)
      self.run_fast_ep(self._scaling_id)
    else:
      logger.warning('Expected output file %s missing', xml_file)

    if allfiles:
      self.record_result_all_files({ 'filelist': allfiles })

    return result['exitcode'] == 0
