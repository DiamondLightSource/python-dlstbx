from __future__ import absolute_import, division, print_function

import json
import logging
import os
import shutil

import dlstbx.util.symlink
from dlstbx.util.merging_statistics import get_merging_statistics
import procrunner
import py
import zocalo.wrapper

logger = logging.getLogger('dlstbx.wrap.fast_dp')

class FastDPWrapper(zocalo.wrapper.BaseWrapper):
  def send_results_to_ispyb(self, z):
    ispyb_command_list = []

    # Step 1: Add new record to AutoProc, keep the AutoProcID
    register_autoproc = {
        'ispyb_command': 'write_autoproc',
        'autoproc_id': None,
        'store_result': 'ispyb_autoproc_id',
        'spacegroup': z['spacegroup'],
        'refinedcell_a': z['unit_cell'][0],
        'refinedcell_b': z['unit_cell'][1],
        'refinedcell_c': z['unit_cell'][2],
        'refinedcell_alpha': z['unit_cell'][3],
        'refinedcell_beta': z['unit_cell'][4],
        'refinedcell_gamma': z['unit_cell'][5],
    }
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

    # Step 3: Store integration result, linked to the ScalingID
    integration = {
        'ispyb_command': 'upsert_integration',
        'scaling_id': '$ispyb_autoprocscaling_id',
        'cell_a': z['unit_cell'][0],
        'cell_b': z['unit_cell'][1],
        'cell_c': z['unit_cell'][2],
        'cell_alpha': z['unit_cell'][3],
        'cell_beta': z['unit_cell'][4],
        'cell_gamma': z['unit_cell'][5],
        'refined_xbeam': z['refined_beam'][0],
        'refined_ybeam': z['refined_beam'][1],
    }
    ispyb_command_list.append(integration)

    logger.info("Sending %s", str(ispyb_command_list))
    self.recwrap.send_to('ispyb', {
        'ispyb_command_list': ispyb_command_list,
    })
    logger.info("Sent %d commands to ISPyB", len(ispyb_command_list))

  def construct_commandline(self, params):
    '''Construct fast_dp command line.
       Takes job parameter dictionary, returns array.'''

    command = [
      'fast_dp', '--atom=S',
      '-j', '0', '-J', '18',
       '-l', 'durin-plugin.so',
      params['fast_dp']['filename']
    ]

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

    # Set appropriate environment variables for forkxds
    environment = {}
    if params.get('forkxds_queue'):
      environment['FORKXDS_QUEUE'] = params['forkxds_queue']
    if params.get('forkxds_project'):
      environment['FORKXDS_PROJECT'] = params['forkxds_project']

    # run fast_dp in working directory
    result = procrunner.run(
        command, timeout=params.get('timeout'),
        print_stdout=False, print_stderr=False,
        working_directory=working_directory.strpath,
        environment_override=environment,
    )

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

      json_file = working_directory.join('iotbx-merging-stats.json')
      with json_file.open('wb') as fh:
        fh.write(get_merging_statistics(
          str(working_directory.join('fast_dp_unmerged.mtz').strpath)).as_json())

    if working_directory.join('fast_dp.error').check():
      result['exitcode'] = 1

    # Create results directory and symlink if they don't already exist
    results_directory.ensure(dir=True)
    if params.get('create_symlink'):
      dlstbx.util.symlink.create_parent_symlink(results_directory.strpath, params['create_symlink'])

    # copy output files to result directory and attach them in ISPyB
    keep_ext = {
      ".cbf": False,
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
      #"fast_dp-report.json": "graph",
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
    if allfiles:
      self.record_result_all_files({ 'filelist': allfiles })

    # Forward JSON results if possible
    if working_directory.join('fast_dp.json').check():
      with working_directory.join('fast_dp.json').open('rb') as fh:
        json_data = json.load(fh)
      self.send_results_to_ispyb(json_data)
    elif result['exitcode']:
      logger.info('fast_dp failed to process the dataset')
    else:
      logger.warning('Expected JSON output file missing')

    return result['exitcode'] == 0
