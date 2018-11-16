from __future__ import absolute_import, division, print_function

import json
import logging
import py
import os

import dlstbx.zocalo.wrapper
import procrunner

logger = logging.getLogger('dlstbx.wrap.autoPROC')

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


class autoPROCWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def send_results_to_ispyb(self, xml_file, use_existing_autoprocprogram_id=True):
    logger.debug("Reading autoPROC results")
    message = xml_to_dict(xml_file)['AutoProcContainer']
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

    # Use existing AutoProcProgramID
    if use_existing_autoprocprogram_id and self.recwrap.environment.get('ispyb_autoprocprogram_id'):
      message['AutoProcProgramContainer']['AutoProcProgram'] = \
        self.recwrap.environment['ispyb_autoprocprogram_id']

    logger.debug("Sending %s", str(message))
    self.recwrap.transport.send('ispyb', message)

    logger.info("Saved autoPROC information for data collection %s", str(dcid))

  def construct_commandline(self, params):
    '''Construct autoPROC command line.
       Takes job parameter dictionary, returns array.'''

    working_directory = params['working_directory']
    image_template = params['autoproc']['image_template']
    image_directory = params['autoproc']['image_directory']
    image_first = params['autoproc']['image_first']
    image_last = params['autoproc']['image_last']
    image_pattern = params['image_pattern']

    beamline = params['beamline']

    prefix = image_template.split('#')[0]
    crystal = prefix.replace('_', '').replace(' ', '').replace('-', '')
    project = os.path.split(image_template)[-2].replace('_', '').replace(' ', '').replace('-', '')

    first_image_path = os.path.join(
      image_directory, image_pattern % int(image_first))

    command = [
      'process', '-xml',
      '-Id', ','.join((crystal, image_directory, image_template, image_first, image_last)),
      'autoPROC_XdsKeyword_MAXIMUM_NUMBER_OF_PROCESSORS=12',
      '-M', 'HighResCutOnCChalf',
      'autoPROC_CreateSummaryImageHrefLink="no"',
      'autoPROC_Summary2Base64_Run="yes"',
      'StopIfSubdirExists="no"',
      '-d', working_directory,
    ]

    if beamline == 'i23':
      command.extend(['-M', 'DiamondI23'])
    elif beamline == 'i04':
      command.extend(['-M', 'DiamondI04'])

    with open(first_image_path, 'rb') as f:
      for line in f.readlines():
        if 'Oscillation_axis' in line and 'SLOW' in line:
          command.append('autoPROC_XdsKeyword_ROTATION_AXIS="0.000000 -1.000000  0.000000"')
          break

    if params.get('ispyb_parameters'):
      if params['ispyb_parameters'].get('d_min'):
        # Can we set just d_min alone?
        # -R <reslow> <reshigh>
        pass
      if params['ispyb_parameters'].get('spacegroup'):
        command.append('symm=%s' % params['ispyb_parameters']['spacegroup'])
      if params['ispyb_parameters'].get('unit_cell'):
        command.append('cell=%s' % params['ispyb_parameters']['unit_cell'])

    return command

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']

    # Adjust all paths if a spacegroup is set in ISPyB
    if params.get('ispyb_parameters'):
      if params['ispyb_parameters'].get('spacegroup') and \
          '/' not in params['ispyb_parameters']['spacegroup']:
        for parameter in ('working_directory', 'results_directory', 'create_symlink'):
          if parameter in params:
            params[parameter] += '-' + params['ispyb_parameters']['spacegroup']
        # only runs without space group are shown in SynchWeb overview
        params['synchweb_ticks'] = None

    command = self.construct_commandline(params)

    working_directory = py.path.local(params['working_directory'])
    results_directory = py.path.local(params['results_directory'])

    # Create working directory with symbolic link
    working_directory.ensure(dir=True)
    if params.get('create_symlink'):
      dlstbx.util.symlink.create_parent_symlink(working_directory.strpath, params['create_symlink'])

    # Create SynchWeb ticks hack file. This will be overwritten with the real log later.
    # For this we need to create the results directory and symlink immediately.
    if params.get('synchweb_ticks'):
      logger.debug('Setting SynchWeb status to swirl')
      if params.get('create_symlink'):
        results_directory.ensure(dir=True)
        dlstbx.util.symlink.create_parent_symlink(results_directory.strpath, params['create_symlink'])
      py.path.local(params['synchweb_ticks']).ensure()

    # disable control sequence parameters from autoPROC output
    # https://www.globalphasing.com/autoproc/wiki/index.cgi?RunningAutoProcAtSynchrotrons#settings

    result = procrunner.run_process(
      command, timeout=params.get('timeout'),
      print_stdout=True, print_stderr=True,
      environment_override={
        'autoPROC_HIGHLIGHT': 'no',
      },
      working_directory=working_directory.strpath)

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])
    with open(os.path.join(working_directory.strpath, 'autoPROC.log'), 'wb') as f:
      f.write(result['stdout'])

    ## http://jira.diamond.ac.uk/browse/I04_1-56 delete softlinks
    #echo "Deleting all soft links found in $localtemp"
    #find $localtemp -type l -exec rm '{}' \;
    #echo "Done deleting"

    #cd $jobdir
    #tar -xzvf summary.tar.gz

    #Visit=`basename ${3}`
    ## put history into the log files
    #echo "Attempting to add history to mtz files"
    #find $jobdir -name '*.mtz' -exec /dls_sw/apps/mx-scripts/misc/AddHistoryToMTZ.sh $Beamline $Visit {} $2 autoPROC \;

    json_file = working_directory.join('iotbx-merging-stats.json')
    scaled_unmerged_mtz = working_directory.join('aimless_unmerged.mtz')
    ispyb_xml = working_directory.join('autoPROC.xml')
    if scaled_unmerged_mtz.check() and ispyb_xml.check():
      self.run_iotbx_merging_statistics(
        scaled_unmerged_mtz.strpath, ispyb_xml.strpath, json_file.strpath)

    # move summary_inlined.html to summary.html
    inlined_html = working_directory.join('summary_inlined.html')
    if inlined_html.check():
      inlined_html.move(working_directory.join('summary.html'))

    # copy output files to result directory
    results_directory.ensure(dir=True)
    if params.get('create_symlink'):
      dlstbx.util.symlink.create_parent_symlink(results_directory.strpath, params['create_symlink'])

    autoproc_xml = working_directory.join('autoPROC.xml')
    ispyb_dls_xml = working_directory.join('ispyb_dls.xml')
    if autoproc_xml.check():
      with autoproc_xml.open('rb') as infile, ispyb_dls_xml.open('wb') as outfile:
        outfile.write(infile.read().replace(
          working_directory.strpath, results_directory.strpath))
      self.fix_xml(ispyb_dls_xml.strpath)

    staraniso_xml = working_directory.join('autoPROC_staraniso.xml')
    staraniso_ispyb_dls_xml = working_directory.join('staraniso_ispyb_dls.xml')
    if staraniso_xml.check():
      with staraniso_xml.open('rb') as infile, staraniso_ispyb_dls_xml.open('wb') as outfile:
        outfile.write(infile.read().replace(
          working_directory.strpath, results_directory.strpath))
      self.fix_xml(staraniso_ispyb_dls_xml.strpath)

    keep_ext = {
      ".INP": None,
      ".xml": None,
      ".png": None,
      ".log": "log",
      ".html": "log",
      ".pdf": "log",
      ".LP": "log",
      ".dat": "result",
      ".HKL": "result",
      ".sca": "result",
      ".mtz": "result"
    }
    keep = {
      "summary.tar.gz": "result",
      "iotbx-merging-stats.json": "graph"
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

    self.send_results_to_ispyb(ispyb_dls_xml.strpath)
    self.send_results_to_ispyb(
      staraniso_ispyb_dls_xml.strpath, use_existing_autoprocprogram_id=False)

    return result['exitcode'] == 0

  @staticmethod
  def run_iotbx_merging_statistics(scaled_unmerged_mtz, ispyb_xml, json_file):
    import iotbx.merging_statistics
    i_obs = iotbx.merging_statistics.select_data(str(scaled_unmerged_mtz), data_labels=None)
    i_obs = i_obs.customized_copy(anomalous_flag=True, info=i_obs.info())
    result = iotbx.merging_statistics.dataset_statistics(
      i_obs=i_obs,
      n_bins=20,
      anomalous=False,
      use_internal_variance=False,
      eliminate_sys_absent=False,
      assert_is_not_unique_set_under_symmetry=False)
    #result.show()
    result.as_json(file_name=json_file)

    from xml.etree import ElementTree
    tree = ElementTree.parse(ispyb_xml)
    root = tree.getroot()
    container = root.find('AutoProcProgramContainer')
    #print container
    #for item in container:
    #  print item
    #help(container)
    attachment = ElementTree.SubElement(container, 'AutoProcProgramAttachment')
    #container.append(attachment)
    fileType = ElementTree.SubElement(attachment, 'fileType')
    fileType.text = 'Graph'
    fileName = ElementTree.SubElement(attachment, 'fileName')
    fileName.text = os.path.basename(json_file)
    filePath = ElementTree.SubElement(attachment, 'filePath')
    filePath.text = os.path.dirname(json_file)
    tree.write(ispyb_xml)

  @staticmethod
  def fix_xml(xml_file):
    from xml.etree import ElementTree

    document = ElementTree.parse(xml_file)

    pattern = '/'.join([
      'AutoProcScalingContainer', 'AutoProcIntegrationContainer',
      'AutoProcIntegration'])
    integration = document.find(pattern)
    if integration is None:
      print('Could not find %s in %s' % (pattern, xml_file))
    else:
      beamX = integration.find('refinedXBeam')
      beamY = integration.find('refinedYBeam')
      # autoPROC swaps X and Y compared to what we expect
      x = float(beamY.text)
      y = float(beamX.text)
      # autoPROC reports beam centre in px rather than mm
      px_to_mm = 0.172
      beamX.text = str(x * px_to_mm)
      beamY.text = str(y * px_to_mm)

    pattern = '/'.join([
      'AutoProcProgramContainer', 'AutoProcProgram',
      'processingPrograms'])
    programs = document.find(pattern)
    staranisoellipsoid = document.find(
      '/'.join(['AutoProcScalingContainer', 'AutoProcScaling',
                'StaranisoEllipsoid']))
    if programs is None:
      print('Could not find %s in %s' % (pattern, xml_file))
    elif staranisoellipsoid is not None:
      programs.text = 'autoPROC+STARANISO'
    else:
      programs.text = 'autoPROC'

    document.write(xml_file)
