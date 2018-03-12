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

    # run xia2 in working directory

    cwd = os.path.abspath(os.curdir)

    working_directory = os.path.abspath(params['working_directory'])
    results_directory = os.path.abspath(params['results_directory'])
    logger.info('working_directory: %s' %working_directory)
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)
    os.chdir(working_directory)

    lifespan = params['strategy']['lifespan']
    transmission = float(params['strategy']['transmission'])
    wavelength = float(params['strategy']['wavelength'])
    beamline = params['strategy']['beamline']
    logger.debug('transmission: %s' %transmission)
    logger.debug('wavelength: %s' %wavelength)
    strategy_lifespan = round((lifespan * (100 / transmission)) * (wavelength/0.979)**-3, 0)
    gentle_strategy_lifespan = round((lifespan * (100 / transmission)) * (wavelength/0.979)**-3 / 10, 0)
    logger.debug('lifespan: %s' %lifespan)

    if beamline == 'i24':
      min_exposure = 0.01
    elif beamline == 'i03':
      min_exposure = 0.01
    else:
      min_exposure = 0.04


    EDNAStrategy1 = os.path.join(working_directory, 'EDNAStrategy1')
    if not os.path.exists(EDNAStrategy1):
      os.mkdir(EDNAStrategy1)
    with open('%s.xml' %EDNAStrategy1, 'wb') as f:
      f.write(self.make_edna_xml(
        complexity=None, multiplicity=3, i_over_sig_i=2,
        lifespan=strategy_lifespan, min_osc_range=0.1,
        min_exposure=min_exposure, anomalous=False))
    short_comments = "EDNAStrategy1 Standard Native Dataset Multiplicity=3 I/sig=2 Maxlifespan=%s s" %strategy_lifespan
    with open(os.path.join(working_directory, 'Strategy.txt'), 'wb') as f:
      f.write(short_comments)

    #
    #Strategy 2 Bog Standard with Anom
    #
    EDNAStrategy2 = os.path.join(working_directory, 'EDNAStrategy2')
    if not os.path.exists(EDNAStrategy2):
      os.mkdir(EDNAStrategy2)
    with open('%s.xml' %EDNAStrategy2, 'wb') as f:
      f.write(self.make_edna_xml(
        complexity=None, multiplicity=3, i_over_sig_i=2,
        lifespan=strategy_lifespan, min_osc_range=0.1,
        min_exposure=min_exposure, anomalous=True))
    short_comments = "EDNAStrategy2 Standard Anomalous Dataset Multiplicity=3 I/sig=2 Maxlifespan=%s s" %strategy_lifespan
    with open(os.path.join(working_directory, 'Strategy.txt'), 'wb') as f:
      f.write(short_comments)

    #
    #Strategy 3 high multiplicity
    #
    EDNAStrategy3 = os.path.join(working_directory, 'EDNAStrategy3')
    if not os.path.exists(EDNAStrategy3):
      os.mkdir(EDNAStrategy3)
    with open('%s.xml' %EDNAStrategy3, 'wb') as f:
      f.write(self.make_edna_xml(
        complexity=None, multiplicity=16, i_over_sig_i=2,
        lifespan=strategy_lifespan, min_osc_range=0.1,
        min_exposure=min_exposure, anomalous=False))
    short_comments = "EDNAStrategy3 strategy with target multiplicity=16 I/sig=2 Maxlifespan=%s s" %strategy_lifespan
    with open(os.path.join(working_directory, 'Strategy.txt'), 'wb') as f:
      f.write(short_comments)

    #
    #Strategy 4
    #
    EDNAStrategy4 = os.path.join(working_directory, 'EDNAStrategy4')
    if not os.path.exists(EDNAStrategy4):
      os.mkdir(EDNAStrategy4)
    with open('%s.xml' %EDNAStrategy4, 'wb') as f:
      f.write(self.make_edna_xml(
        complexity=None, multiplicity=2, i_over_sig_i=2,
        lifespan=gentle_strategy_lifespan, min_osc_range=0.1,
        min_exposure=min_exposure, anomalous=False))
    short_comments = "EDNAStrategy4 Gentle: Target Multiplicity=2 and target I/Sig 2 Maxlifespan=%s s" %gentle_strategy_lifespan
    with open(os.path.join(working_directory, 'Strategy.txt'), 'wb') as f:
      f.write(short_comments)

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
