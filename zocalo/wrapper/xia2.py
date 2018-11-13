from __future__ import absolute_import, division, print_function

import glob
import logging
import os
import shutil

import dlstbx.util.symlink
import dlstbx.zocalo.wrapper
import procrunner

logger = logging.getLogger('dlstbx.wrap.xia2')

class Xia2Wrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def construct_commandline(self, params):
    '''Construct xia2 command line.
       Takes job parameter dictionary, returns array.'''

    command = ['xia2']

    for param, values in params['xia2'].iteritems():
      if param == 'images':
        if not values:
          # This may be empty if related data collections are requested, but no related DCs were found
          continue
        param = 'image'
        values = values.split(',')
      if not isinstance(values, (list, tuple)):
        values = [values]
      for v in values:
        command.append('%s=%s' % (param, v))

    if params.get('ispyb_parameters'):
      if params['ispyb_parameters'].get('d_min'):
        command.append('xia2.settings.resolution.d_min=%s' % \
                       params['ispyb_parameters']['d_min'])
      if params['ispyb_parameters'].get('spacegroup'):
        command.append('xia2.settings.space_group=%s' % \
                       params['ispyb_parameters']['spacegroup'])
      if params['ispyb_parameters'].get('unit_cell'):
        command.append('xia2.settings.unit_cell=%s' % \
                       params['ispyb_parameters']['unit_cell'])

    return command

  def send_results_to_ispyb(self):
    logger.info("Reading xia2 results")
    from xia2.command_line.ispyb_json import zocalo_object
    z = zocalo_object()

    ispyb_command_list = []

    # Step 1: Add new record to AutoProc, keep the AutoProcID
    register_autoproc = z['refined_results']
    register_autoproc.update({
        'ispyb_command': 'write_autoproc',
        'autoproc_id': None,
        'store_result': 'ispyb_autoproc_id',
    })
    ispyb_command_list.append(register_autoproc)

    # Step 2: Store scaling results, linked to the AutoProcID
    #         Keep the AutoProcScalingID
    insert_scaling = z['scaling_statistics']
    insert_scaling.update({
        'ispyb_command': 'insert_scaling',
        'autoproc_id': '$ispyb_autoproc_id',
        'store_result': 'ispyb_autoprocscaling_id',
    })
    ispyb_command_list.append(insert_scaling)

    # Step 3: Store integration results, linking them to ScalingID
    for n, integration in enumerate(z['integrations']):
      integration.update({
          'ispyb_command': 'upsert_integration',
          'scaling_id': '$ispyb_autoprocscaling_id',
      })
      if n > 0:
        # make sure only the first integration uses a specified integration ID
        # and all subsequent integration results are written to a new record
        integration['integration_id'] = None
      ispyb_command_list.append(integration)

    logger.info("Sending %s", str(ispyb_command_list))
    self.recwrap.send_to('ispyb', {
        'ispyb_command_list': ispyb_command_list,
    })
    logger.info("Sent %d commands to ISPyB", len(ispyb_command_list))

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    command = self.construct_commandline(params)

    working_directory = params['working_directory']
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)

    result = procrunner.run_process(
      command, timeout=params.get('timeout'),
      print_stdout=False, print_stderr=False,
      working_directory=working_directory)

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    # copy output files to result directory

    results_directory = params['results_directory']
    if params['ispyb_parameters'].get('spacegroup'):
      results_directory += params['ispyb_parameters'].get('spacegroup')
    if not os.path.exists(results_directory):
      os.makedirs(results_directory)

    for subdir in ('DataFiles', 'LogFiles'):
      src = os.path.join(working_directory, subdir)
      dst = os.path.join(results_directory, subdir)
      if os.path.exists(src):
        logger.debug('Copying %s to %s' % (src, dst))
        shutil.copytree(src, dst)
      elif result['exitcode']:
        logger.info('Expected output directory does not exist (non-zero exitcode): %s', src)
      else:
        logger.warning('Expected output directory does not exist: %s', src)

    allfiles = []
    for f in glob.glob(os.path.join(working_directory, '*.*')):
      shutil.copy(f, results_directory)
      allfiles.append(os.path.join(results_directory, os.path.basename(f)))

    # Send results to various listeners

    # Part of the result parsing requires to be in result directory
    cwd = os.path.abspath(os.curdir)
    os.chdir(results_directory)

    if params.get('results_symlink'):
      # Create symbolic link above working directory
      dlstbx.util.symlink.create_parent_symlink(results_directory, params['results_symlink'])

    if not result['exitcode'] and not os.path.isfile('xia2.error') and os.path.exists('xia2.json') \
        and not params.get('do_not_write_to_ispyb'):
      self.send_results_to_ispyb()

    logfiles = [ 'xia2.html', 'xia2.error' ]
    for result_file in filter(os.path.isfile, logfiles):
      self.record_result_individual_file({
        'file_path': results_directory,
        'file_name': os.path.basename(result_file),
        'file_type': 'log',
      })

    datafiles_path = os.path.join(results_directory, 'DataFiles')
    if os.path.exists(datafiles_path):
      for result_file in os.listdir(datafiles_path):
        file_type = 'result'
        if result_file.endswith(('.log', '.txt')):
          file_type = 'log'
        self.record_result_individual_file({
          'file_path': datafiles_path,
          'file_name': result_file,
          'file_type': file_type,
        })
        allfiles.append(os.path.join(datafiles_path, result_file))

    logfiles_path = os.path.join(results_directory, 'LogFiles')
    if os.path.exists(logfiles_path):
      for result_file in os.listdir(logfiles_path):
        file_type = 'log'
        if result_file.endswith(('.png',)):
          file_type = 'graph'
        self.record_result_individual_file({
          'file_path': logfiles_path,
          'file_name': result_file,
          'file_type': file_type,
        })
        allfiles.append(os.path.join(logfiles_path, result_file))

    if allfiles:
      self.record_result_all_files({ 'filelist': allfiles })

    os.chdir(cwd)

    return result['exitcode'] == 0
