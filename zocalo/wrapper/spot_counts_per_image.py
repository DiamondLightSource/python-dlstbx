from __future__ import absolute_import, division, print_function

import itertools
import json
import logging
import os
import shutil

from procrunner import run_process
import zocalo.wrapper

logger = logging.getLogger('dlstbx.wrap.spot_counts_per_image')

class SCPIWrapper(zocalo.wrapper.BaseWrapper):
  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']

    # run in working directory
    working_directory = params['working_directory']
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)
    os.chdir(working_directory)

    prefix = 'pia'

    if os.getenv('NSLOTS') or params.get('nproc'):
      nproc = [ "nproc=" + str(os.getenv('NSLOTS') or params.get('nproc')) ]
    else:
      nproc = []

    for command in (
          ['dials.import', params['data'] ],
          ['dials.find_spots', 'datablock.json'] + nproc,
          ['dials.spot_counts_per_image', 'datablock.json', 'strong.pickle',
           'json=%s.json' % prefix, 'joint_json=True', 'split_json=True'],
        ):
      result = run_process(
        command, timeout=params.get('timeout'),
        print_stdout=False, print_stderr=False)

      logger.info('command: %s', ' '.join(result['command']))
      logger.info('timeout: %s', result['timeout'])
      logger.info('time_start: %s', result['time_start'])
      logger.info('time_end: %s', result['time_end'])
      logger.info('runtime: %s', result['runtime'])
      logger.info('exitcode: %s', result['exitcode'])
      logger.debug(result['stdout'])
      logger.debug(result['stderr'])
      if result['exitcode'] != 0:
        logger.error('Spot counting failed on %s during step %s', filename, command[0])
        break

    success = result['exitcode'] == 0

    # copy output files to result directory
    results_directory = params['results_directory']
    if not os.path.exists(results_directory):
      os.makedirs(results_directory)

    defaultfiles = ('estimated_d_min', 'n_spots_total')
    foundfiles = []
    filesmissing = False
    for filename in params.get('keep_files', defaultfiles):
      filename = prefix + '_' + filename + '.json'

      if os.path.exists(filename):
        dst = os.path.join(results_directory, filename)
        logger.debug('Copying %s to %s' % (filename, dst))
        shutil.copy(filename, dst)
        foundfiles.append(dst)
        self.record_result_individual_file({
          'file_path': results_directory,
          'file_name': filename,
          'file_type': 'pia',
        })
      else:
        filesmissing = True
        if success:
          logger.warning('Expected output file %s missing', filename)
        else:
          logger.info('Expected output file %s missing', filename)
    success = success and not filesmissing

    if foundfiles:
      logger.info('Notifying for found files: %s', str(foundfiles))
      self.record_result_all_files({ 'filelist': foundfiles })

    # Identify selection of PIA results to send on
    selections = [ k for k in self.recwrap.recipe_step['output'].iterkeys()
                   if isinstance(k, basestring) and k.startswith('select-') ]
    selections = { int(k[7:]): k for k in selections }

    logger.info('Processing grouped per-image-analysis statistics')
    json_data = {'total_intensity': []}
    if os.path.exists('%s.json' % prefix):
      with open('%s.json' % prefix) as fp:
        json_data = json.load(fp)
    pia_keys = json_data.keys()
    imagecount = len(json_data['total_intensity'])
    for filenumber, image_values in enumerate(itertools.izip(*json_data.itervalues()), 1):
      pia = dict(zip(pia_keys, image_values))
      pia['file-number'] = filenumber

      # Send result for every image
      self.recwrap.send_to('every', pia)
      print("Every:", pia)

      # Send result for image selections
      for m, dest in selections.iteritems():
        if filenumber in (
            imagecount,
            1 + round(filenumber * (m-1) // imagecount) \
                * imagecount // (m-1)):
          self.recwrap.send_to(dest, pia)
          print("Select:", pia)
    logger.info('Done.')

    return success
