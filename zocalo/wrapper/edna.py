from __future__ import absolute_import, division, print_function

import glob
import logging
import os
import py
import sys

import procrunner
import zocalo.wrapper

logger = logging.getLogger('dlstbx.wrap.edna')

class EdnaWrapper(zocalo.wrapper.BaseWrapper):

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    working_directory = py.path.local(params['working_directory'])
    results_directory = py.path.local(params['results_directory'])
    logger.info('working_directory: %s' % working_directory.strpath)
    working_directory.ensure(dir=True)
    try: # set Synchweb to swirl
     results_directory.join('summary.html').ensure()
    except OSError:
      pass # it'll be fine

    if params['image_template'].endswith('.h5'):
      edna_module = 'edna/mx-20190213-auto'
      self.hdf5_to_cbf()
    else:
      self.generate_modified_headers()
      edna_module = 'edna/20140709-auto'

    sparams = params['strategy']
    lifespan = sparams['lifespan']
    transmission = float(sparams['transmission'])
    wavelength = float(sparams['wavelength'])
    beamline = sparams['beamline']
    logger.debug('transmission: %s' %transmission)
    logger.debug('wavelength: %s' %wavelength)
    strategy_lifespan = round((lifespan * (100 / transmission)) * (wavelength/0.979)**-3, 0)
    gentle_strategy_lifespan = round((lifespan * (100 / transmission)) * (wavelength/0.979)**-3 / 10, 0)
    logger.debug('lifespan: %s' %lifespan)

    min_exposure = sparams['min_exposure'].get(
      beamline, sparams['min_exposure']['default'])

    multiplicity = sparams['multiplicity']
    i_over_sig_i = sparams['i_over_sig_i']
    EDNAStrategy = working_directory.join('EDNAStrategy')
    EDNAStrategy.ensure(dir=True)
    with open('%s.xml' %EDNAStrategy, 'wb') as f:
      f.write(self.make_edna_xml(
        complexity='none', multiplicity=multiplicity,
        i_over_sig_i=i_over_sig_i,
        lifespan=strategy_lifespan, min_osc_range=0.1,
        min_exposure=min_exposure, anomalous=sparams['anomalous']))
    short_comments = "%s Multiplicity=%s I/sig=%s Maxlifespan=%s s" %(
      sparams['description'], multiplicity, i_over_sig_i, strategy_lifespan)
    with working_directory.join('Strategy.txt').open('wb') as f:
      f.write(short_comments)

    edna_home = os.environ['EDNA_HOME']
    strategy_xml = working_directory.join('EDNAStrategy.xml')
    results_xml = working_directory.join('results.xml')
    wrap_edna_sh = working_directory.join('wrap_edna.sh')
    with wrap_edna_sh.open('wb') as f:
      if beamline == 'i24':
        edna_site = 'export EDNA_SITE=DLS_i24'
      else:
        edna_site = ''
      f.write('''\
module load global/cluster
module load %(edna_module)s
export DCID=%(dcid)s
export COMMENTS="%(comments)s"
export SHORT_COMMENTS="%(short_comments)s"
%(edna_site)s
${EDNA_HOME}/kernel/bin/edna-plugin-launcher \
  --execute EDPluginControlInterfacev1_2 --DEBUG \
  --inputFile %(input_file)s \
  --outputFile %(output_file)s''' % dict(
        comments=short_comments,
        short_comments=sparams['name'],
        edna_module=edna_module,
        dcid=params['dcid'],
        edna_site=edna_site,
        input_file=strategy_xml,
        output_file=results_xml,
      ))
    commands = [
      'sh', wrap_edna_sh.strpath,
      strategy_xml.strpath, results_xml.strpath]
    logger.info(' '.join(commands))
    result = procrunner.run(
      commands,
      working_directory=EDNAStrategy.strpath,
      timeout=params.get('timeout', 3600),
      environment_override={
          'LD_LIBRARY_PATH': '',
          'LOADEDMODULES': '',
          'PYTHONPATH': '',
          '_LMFILES_': '',
      },
    )

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    # generate two different html pages
    # not sure which if any of these are actually used/required
    edna2html = os.path.join(edna_home, 'libraries/EDNA2html-0.0.10a/EDNA2html')
    commands = [
      edna2html,
      '--title="%s"' % short_comments,
      '--run_basename=%s/EDNAStrategy' % working_directory.strpath,
      '--portable',
      '--basename=%s/summary' % working_directory.strpath
    ]
    result = procrunner.run(
      commands,
      working_directory=working_directory.strpath,
      timeout=params.get('timeout', 3600),
      print_stdout=True, print_stderr=True,
    )

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    self.edna2html(results_xml)

    # copy output files to result directory
    logger.info('Copying results from %s to %s' % (
                working_directory.strpath, results_directory.strpath))

    source_dir = working_directory / 'EDNAStrategy'
    dest_dir = results_directory / ('EDNA%s' % sparams['name'])
    source_dir.copy(dest_dir)
    src = working_directory / 'EDNAStrategy.xml'
    dst = results_directory / ('EDNA%s.xml' % sparams['name'])
    src.copy(dst)
    for fname in ('summary.html', 'results.xml'):
      src = working_directory / fname
      dst = results_directory / fname
      if src.check() and (not dst.check() or dst.size() == 0):
        src.copy(dst)
    return result['exitcode'] == 0

  def hdf5_to_cbf(self):
    params = self.recwrap.recipe_step['job_parameters']
    working_directory = py.path.local(params['working_directory'])
    tmpdir = working_directory.join('image-tmp')
    tmpdir.ensure(dir=True)
    master_h5 = os.path.join(params['image_directory'], params['image_template'])
    prefix = params['image_template'].split('master.h5')[0]
    params['image_pattern'] = prefix + '%04d.cbf'
    logger.info('Image pattern: %s', params['image_pattern'])
    logger.info(
      'Converting %s to %s' % (master_h5, tmpdir.join(params['image_pattern'])))
    result = procrunner.run(
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
    params['image_directory'] = tmpdir

  def generate_modified_headers(self,):
    params = self.recwrap.recipe_step['job_parameters']

    def behead(cif_in, cif_out):
      logger.info('Writing modified file %s to %s' % (cif_in, cif_out.strpath))
      assert os.path.exists(cif_in)
      assert not cif_out.check()

      data = open(cif_in, 'rb').read()

      if '# This and all subsequent lines will' in data:
        head = data.split('# This and all subsequent lines will')[0]
        tail = data.split('CBF_BYTE_OFFSET little_endian')[-1]
        data = head + tail

      cif_out.write_binary(data)

    working_directory = py.path.local(params['working_directory'])
    tmpdir = working_directory.join('image-tmp')
    tmpdir.ensure(dir=True)

    template = os.path.join(params['image_directory'], params['image_template'])

    g = glob.glob(template.replace('#', '?'))
    logger.info(template)
    logger.info(g)
    for f in g:
      behead(f, tmpdir.join(os.path.basename(f)))

    params['orig_image_directory'] = params['image_directory']
    params['image_directory'] = tmpdir

  def make_edna_xml(self, complexity, multiplicity, i_over_sig_i,
                    lifespan, min_osc_range, min_exposure, anomalous=False):

    params = self.recwrap.recipe_step['job_parameters']
    dcid = int(params['dcid'])
    assert dcid > 0, 'Invalid data collection ID given.'

    anomalous = 1 if anomalous else 0

    #1) Echo out the header
    output = '<?xml version="1.0" ?><XSDataInputInterfacev2_2>'

    #2) Echo out the diffractionPlan
    output = output + '''
<diffractionPlan>
  <anomalousData>
    <value>%(anomalous)i</value>
  </anomalousData>
  <complexity>
    <value>%(complexity)s</value>
  </complexity>
  <aimedIOverSigmaAtHighestResolution>
    <value>%(i_over_sig_i)s</value>
  </aimedIOverSigmaAtHighestResolution>
  <aimedMultiplicity>
    <value>%(multiplicity)s</value>
  </aimedMultiplicity>
  <minExposureTimePerImage>
    <value>%(min_exposure)s</value>
  </minExposureTimePerImage>
  <maxExposureTimePerDataCollection>
    <value>%(lifespan)s</value>
  </maxExposureTimePerDataCollection>
''' % dict(anomalous=anomalous,
           complexity=complexity,
           i_over_sig_i=i_over_sig_i,
           multiplicity=multiplicity,
           min_exposure=min_exposure,
           lifespan=lifespan)

    #logger.info('spacegroup: %s' %params.get('spacegroup'))
    #space_group = params.get('spacegroup')
    #if space_group is not None:
    #  print >> s, """            <forcedSpaceGroup>
    #              <value>%s</value>
    #          </forcedSpaceGroup>""" %space_group
    output = output + '</diffractionPlan>'

    #3) Echo out the full path for each image.

    logger.info(str(params.keys()))
    image_directory = params['image_directory']
    image_first = int(params['image_first'])
    image_last = int(params['image_last'])

    # image_pattern doesn't work: jira.diamond.ac.uk/browse/SCI-6131
    image_pattern = params['image_pattern']
    #template = params['image_template']
    #fmt = '%%0%dd' % template.count('#')
    #prefix = template.split('#')[0]
    #suffix = template.split('#')[-1]
    #image_pattern = prefix + fmt + suffix

    logger.info('%s %s:%s' %(image_pattern, image_first, image_last))
    for i_image in range(image_first, image_last+1):
      image_file_name = image_directory.join(image_pattern % i_image)
      output = output + '''
<imagePath><path><value>%s</value></path></imagePath>
''' % image_file_name

    #4) Echo out the beam and flux (if we know them)
    flux = params['strategy']['flux']
    try:
      flux = float(flux)
    except ValueError:
      flux = None
    beam_size_x = float(params['strategy']['beam_size_x'])
    beam_size_y = float(params['strategy']['beam_size_y'])
    if flux:
      output = output + "<flux><value>%s</value></flux>" % flux
    if beam_size_x:
      output = output + "<beamSizeX><value>%s</value></beamSizeX>" % beam_size_x
    if beam_size_y:
      output = output + "<beamSizeY><value>%s</value></beamSizeY>" % beam_size_y

    #5) Echo out omega,kappa,phi (if we know them)
    for axis in ('chi', 'kappa', 'omega', 'phi'):
      angle = params['strategy'].get(axis)
      if angle is not None:
        output = output + "<%s><value>%s</value></%s>" % (axis, angle, axis)

    #6) and close
    output = output + "</XSDataInputInterfacev2_2>"

    return output

  @staticmethod
  def edna2html(result_xml):
    sys.path.append(os.path.join(os.environ["EDNA_HOME"],"kernel","src"))
    from EDFactoryPluginStatic import EDFactoryPluginStatic
    EDFactoryPluginStatic.loadModule("XSDataInterfacev1_2")
    from XSDataInterfacev1_2 import XSDataResultInterface
    xsDataResultInterface = XSDataResultInterface.parseFile(result_xml.strpath)
    characterisationResult = xsDataResultInterface.resultCharacterisation
    EDFactoryPluginStatic.loadModule("XSDataSimpleHTMLPagev1_0")
    from XSDataSimpleHTMLPagev1_0 import XSDataInputSimpleHTMLPage
    xsDataInputSimpleHTMLPage = XSDataInputSimpleHTMLPage()
    xsDataInputSimpleHTMLPage.characterisationResult = characterisationResult
    edPluginHTML = EDFactoryPluginStatic.loadPlugin("EDPluginExecSimpleHTMLPagev1_0")
    edPluginHTML.dataInput = xsDataInputSimpleHTMLPage
    edPluginHTML.executeSynchronous()
    xsDataResult = edPluginHTML.dataOutput
    logger.info(xsDataResult.pathToHTMLFile.path.value)
