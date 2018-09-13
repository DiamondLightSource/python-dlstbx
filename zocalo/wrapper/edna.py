from __future__ import absolute_import, division, print_function

import glob
import logging
import os
import shutil

import dlstbx.zocalo.wrapper
import procrunner

logger = logging.getLogger('dlstbx.wrap.xia2_strategy')

class EdnaWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def construct_commandline(self, params):
    '''Construct EDNA command line.
       Takes job parameter dictionary, returns array.'''

    command = ['edna']

    return command

  def send_results_to_ispyb(self):
    return

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    command = self.construct_commandline(params)
    logger.info(command)

    cwd = os.path.abspath(os.curdir)

    working_directory = os.path.abspath(params['working_directory'])
    results_directory = os.path.abspath(params['results_directory'])
    logger.info('working_directory: %s' %working_directory)
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)
    os.chdir(working_directory)

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
    EDNAStrategy = os.path.join(working_directory, 'EDNAStrategy')
    if not os.path.exists(EDNAStrategy):
      os.mkdir(EDNAStrategy)
    with open('%s.xml' %EDNAStrategy, 'wb') as f:
      f.write(self.make_edna_xml(
        complexity='none', multiplicity=multiplicity,
        i_over_sig_i=i_over_sig_i,
        lifespan=strategy_lifespan, min_osc_range=0.1,
        min_exposure=min_exposure, anomalous=sparams['anomalous']))
    short_comments = "%s Multiplicity=%s I/sig=%s Maxlifespan=%s s" %(
      sparams['description'], multiplicity, i_over_sig_i, strategy_lifespan)
    with open(os.path.join(working_directory, 'Strategy.txt'), 'wb') as f:
      f.write(short_comments)

    os.chdir(EDNAStrategy)
    os.environ['DCID'] = params['dcid']
    os.environ['SHORT_COMMENTS'] = sparams['name']
    os.environ['COMMENTS'] = short_comments
    edna_home = os.environ['EDNA_HOME']
    strategy_xml = os.path.join(working_directory, 'EDNAStrategy.xml')
    results_xml = os.path.join(working_directory, 'results.xml')
    commands = ['%s/kernel/bin/edna-plugin-launcher' % edna_home,
       '--execute', 'EDPluginControlInterfacev1_2', '--DEBUG',
       '--inputFile', strategy_xml,
       '--outputFile', results_xml]
    result = procrunner.run_process(
      commands,
      timeout=params.get('timeout', 3600),
      print_stdout=True, print_stderr=True)

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
      '--run_basename=%s/EDNAStrategy' % working_directory,
      '--portable',
      '--basename=%s/summary' % working_directory
    ]
    result = procrunner.run_process(
      commands,
      timeout=params.get('timeout', 3600),
      print_stdout=True, print_stderr=True)

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    os.chdir(cwd)

    self.edna2html(results_xml)

    return result['exitcode'] == 0

  def make_edna_xml(self, complexity, multiplicity, i_over_sig_i,
                    lifespan, min_osc_range, min_exposure, anomalous=False):

    params = self.recwrap.recipe_step['job_parameters']
    dcid = int(params['dcid'])
    assert dcid > 0, 'Invalid data collection ID given.'

    anomalous = 1 if anomalous else 0

    from cStringIO import StringIO
    s = StringIO()
    #1) Echo out the header
    s.write("""<?xml version=\"1.0\" ?>
<XSDataInputInterfacev2_2>""")

    #2) Echo out the diffractionPlan
    s.write("""        <diffractionPlan>
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
""" %dict( anomalous=anomalous,
           complexity=complexity,
           i_over_sig_i=i_over_sig_i,
           multiplicity=multiplicity,
           min_exposure=min_exposure,
           lifespan=lifespan))

    #logger.info('spacegroup: %s' %params.get('spacegroup'))
    #space_group = params.get('spacegroup')
    #if space_group is not None:
    #  print >> s, """            <forcedSpaceGroup>
    #              <value>%s</value>
    #          </forcedSpaceGroup>""" %space_group
    print("        </diffractionPlan>", file=s)


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
      image_file_name = os.path.join(image_directory, image_pattern % i_image)
      print("""    <imagePath>
            <path>
                <value>%s</value>
            </path>
        </imagePath>""" %image_file_name, file=s)

    #4) Echo out the beam and flux (if we know them)
    flux = float(params['strategy']['flux'])
    beam_size_x = float(params['strategy']['beam_size_x'])
    beam_size_y = float(params['strategy']['beam_size_y'])
    if flux:
      print("""    <flux>
            <value>%s</value>
        </flux>""" %flux, file=s)
    if beam_size_x:
      print("""    <beamSizeX>
            <value>%s</value>
        </beamSizeX>""" %beam_size_x, file=s)
    if beam_size_y:
      print("""    <beamSizeY>
            <value>%s</value>
        </beamSizeY>""" %beam_size_y, file=s)

#    #5) Echo out omega,kappa,phi (if we know them)
#    if [ "${Omega}" != "" ] ; then
#    echo "    <omega>
#            <value>${Omega}</value>
#        </omega>"
#    fi
#    if [ "${Kappa}" != "" ] ; then
#    echo "    <kappa>
#            <value>${Kappa}</value>
#        </kappa>"
#    fi
#    if [ "${Phi}" != "" ] ; then
#    echo "    <phi>
#            <value>${Phi}</value>
#        </phi>"
#    fi

    #6) and close
    print("</XSDataInputInterfacev2_2>", file=s)

    return s.getvalue()

  @staticmethod
  def edna2html(result_xml):
    import os, sys
    sys.path.append(os.path.join(os.environ["EDNA_HOME"],"kernel","src"))
    from EDFactoryPluginStatic import EDFactoryPluginStatic
    EDFactoryPluginStatic.loadModule("XSDataInterfacev1_2")
    from XSDataInterfacev1_2 import XSDataResultInterface
    xsDataResultInterface = XSDataResultInterface.parseFile(result_xml)
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
