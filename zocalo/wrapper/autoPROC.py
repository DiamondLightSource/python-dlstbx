from __future__ import absolute_import, division, print_function

import json
import logging
import os
import shutil

import dlstbx.zocalo.wrapper
import procrunner

logger = logging.getLogger('dlstbx.wrap.autoPROC')

class autoPROCWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def send_results_to_ispyb(self, xml_file):
    pass

  def construct_commandline(self, params):
    '''Construct autoPROC command line.
       Takes job parameter dictionary, returns array.'''

    image_template = params['autoproc']['image_template']
    image_directory = params['autoproc']['image_directory']
    image_first = params['autoproc']['image_first']
    image_last = params['autoproc']['image_last']
    image_pattern = params['image_pattern']

    beamline = params['beamline']

    prefix = image_template.split('#')[0]
    crystal = prefix.replace('_', '').replace(' ', '').replace('-', '')
    project = os.path.split(image_template)[-2].replace('_', '').replace(' ', '').replace('-', '')

    first_image_path = image_pattern % int(image_first)
    rotation_axis = ''
    with open(first_image_path, 'rb') as f:
      for line in f.readlines():
        if 'Oscillation_axis' in line and 'SLOW' in line:
          rotation_axis = 'autoPROC_XdsKeyword_ROTATION_AXIS="0.000000 -1.000000  0.000000"'

    if beamline == "i23":
      beamline_macro = "-M DiamondI23"
    elif beamline == "i04":
      beamline_macro = "-M DiamondI04"
    else:
      beamline_macro = ""

    command = [
      'process', '-xml', beamline_macro,
      '-Id', ','.join((crystal, image_directory, image_template, image_first, image_last)),
      'autoPROC_XdsKeyword_MAXIMUM_NUMBER_OF_PROCESSORS=12',
      '-M', 'HighResCutOnCChalf',
      'autoPROC_CreateSummaryImageHrefLink="no"',
      'autoPROC_Summary2Base64_Run="yes"',
      rotation_axis,
      'StopIfSubdirExists="no"',
      '-d', working_directory,
    ]
    return command

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    command = self.construct_commandline(params)

    # run autoPROC in working directory
    working_directory = params['working_directory']
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)

    # disable control sequence parameters from autoPROC output
    # https://www.globalphasing.com/autoproc/wiki/index.cgi?RunningAutoProcAtSynchrotrons#settings
    environment_override={
      'autoPROC_HIGHLIGHT': 'no',
    },

    result = procrunner.run_process(
      command, timeout=params.get('timeout'),
      print_stdout=False, print_stderr=False,
      environment_override=environment_override,
      working_directory=working_directory)

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    #Visit=`basename ${3}`
    ## put history into the log files
    #echo "Attempting to add history to mtz files"
    #find $jobdir -name '*.mtz' -exec /dls_sw/apps/mx-scripts/misc/AddHistoryToMTZ.sh $Beamline $Visit {} $2 autoPROC \;

    json_file = os.path.join(working_directory, 'iotbx-merging-stats.json')
    scaled_unmerged_mtz = os.path.join(working_directory, 'aimless_unmerged.mtz')
    ispyb_xml = os.path.join(working_directory, 'autoPROC.xml')
    if os.path.exists(scaled_unmerged_mtz) and os.path.exists(ispyb_xml):
      self.run_iotbx_merging_statistics(
        scaled_unmerged_mtz, ispyb_xml, json_file)

    # move summary_inlined.html to summary.html
    inlined_html = os.path.join(working_directory, 'summary_inlined.html')
    if os.path.exists(inlined_html):
      shutil.move(inlined_html, os.path.join(working_directory, 'summary.html'))

    autoproc_xml = os.path.join(working_directory, 'autoPROC.xml')
    ispyb_dls_xml = os.path.join(working_directory, 'ispyb_dls.xml')
    if os.path.exists(autoproc_xml):
      with open(autoproc_xml, 'rb') as infile, open(ispyb_dls_xml, 'wb') as outfile:
        outfile.write(infile.read().replace(
          working_directory, results_directory))
      self.fix_xml(ispyb_dls_xml)

    staraniso_xml = os.path.join(working_directory, 'autoPROC_staraniso.xml')
    staraniso_ispyb_dls_xml = os.path.join(working_directory, 'staraniso_ispyb_dls.xml')
    if os.path.exists(staraniso_xml):
      with open(staraniso_xml, 'rb') as infile, open(staraniso_ispyb_dls_xml, 'wb') as outfile:
        outfile.write(infile.read().replace(
          working_directory, results_directory))
      self.fix_xml(staraniso_ispyb_dls_xml)

    # copy output files to result directory
    results_directory = params['results_directory']
    if not os.path.exists(results_directory):
      os.makedirs(results_directory)

    keep_ext = {
      ".INP": None,
      ".xml": None,
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
      "summary.tar.gz": "result,
      "iotbx-merging-stats.json": "graph"
    }
    files = os.listdir(working_directory)
    for filename in files:
      ext = os.path.splitext(filename)[-1]
      if ext in keep_ext:
        keep[filename] = keep_ext[ext]

    allfiles = []
    for filename, filetype in keep.iteritems():
      filenamefull = os.path.join(working_directory, filename)
      if os.path.exists(filenamefull):
        dst = os.path.join(results_directory, filename)
        logger.debug('Copying %s to %s' % (filenamefull, dst))
        shutil.copy(filenamefull, dst)
        allfiles.append(dst)
        if filetype is not None:
          self.record_result_individual_file({
            'file_path': results_directory,
            'file_name': filename,
            'file_type': filetype,
          })

    self.send_results_to_ispyb(ispyb_dls_xml)
    self.send_results_to_ispyb(staraniso_ispyb_dls_xml)

    if allfiles:
      self.record_result_all_files({ 'filelist': allfiles })

    return result['exitcode'] == 0

  def run_iotbx_merging_statistics(scaled_unmerged_mtz, ispyb_xml, json_file):
    import iotbx.merging_statistics
    i_obs = iotbx.merging_statistics.select_data(scaled_unmerged_mtz, data_labels=None)
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

  def fix_xml(xml_file):
    from xml.etree import ElementTree

    document = ElementTree.parse(xml_file)

    pattern = '/'.join([
      'AutoProcScalingContainer', 'AutoProcIntegrationContainer',
      'AutoProcIntegration'])
    integration = document.find(pattern)
    if integration is None:
      print 'Could not find %s in %s' %(pattern, infile)
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
      print 'Could not find %s in %s' %(pattern, infile)
    elif staranisoellipsoid is not None:
      programs.text = 'autoPROC+STARANISO'
    else:
      programs.text = 'autoPROC'

    document.write(xml_file)
