from __future__ import absolute_import, division, print_function

import logging
import os
import py

import dlstbx.zocalo.wrapper
import procrunner

logger = logging.getLogger('dlstbx.wrap.fast_ep')

class FastEPWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def check_go_fast_ep(self, params):
    command = ['go_fast_ep', params['fast_ep']['data']]
    result = procrunner.run_process(
      command, timeout=params.get('timeout'),
      print_stdout=True, print_stderr=True,
      working_directory=params['working_directory'])
    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    go_fast_ep = result['exitcode'] == 0
    if go_fast_ep:
      logger.info('Computer says go for fast_ep :)')
    else:
      logger.info('Computer says no for fast_ep :(')
    return go_fast_ep

  def construct_commandline(self, params):
    '''Construct fast_ep command line.
       Takes job parameter dictionary, returns array.'''

    command = ['fast_ep']
    for param, value in params['fast_ep'].iteritems():
      logging.info('Parameter %s: %s' % (param, str(value)))
      if param == 'rlims':
        value = ','.join([str(r) for r in value])
      command.append('%s=%s' % (param, value))
    command.append('xml=fast_ep.xml')

    return command

  def send_results_to_ispyb(self, xml_file):
    params = self.recwrap.recipe_step['job_parameters']
    command = [
      'python',
      '/dls_sw/apps/mx-scripts/dbserver/src/phasing2ispyb.py',
      '-s', 'sci-serv3', '-p', '1994', '--fix_sgids', '-d',
      '-i', xml_file,
      '-f', params['fast_ep']['data'],
      '-o', os.path.join(params['working_directory'], 'fast_ep_ispyb_ids.xml')
    ]

    result = procrunner.run_process(
      command, timeout=params.get('timeout'),
      print_stdout=True, print_stderr=True,
      working_directory=params['working_directory'])

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    return result['exitcode'] == 0

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    if 'ispyb_parameters' in params:
      if params['ispyb_parameters'].get('data'):
        params['fast_ep']['data'] = os.path.abspath(params['ispyb_parameters']['data'])
      if params['ispyb_parameters'].get('check_go_fast_ep'):
        go_fast_ep = self.check_go_fast_ep(params)
        if not go_fast_ep:
          return
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

    result = procrunner.run_process(
      command, timeout=params.get('timeout'),
      print_stdout=False, print_stderr=False,
      working_directory=working_directory.strpath)

    logger.info('command: %s', ' '.join(result['command']))
    logger.info('timeout: %s', result['timeout'])
    logger.info('time_start: %s', result['time_start'])
    logger.info('time_end: %s', result['time_end'])
    logger.info('runtime: %s', result['runtime'])
    logger.info('exitcode: %s', result['exitcode'])
    logger.debug(result['stdout'])
    logger.debug(result['stderr'])

    # Create results directory and symlink if they don't already exist
    results_directory.ensure(dir=True)
    if params.get('create_symlink'):
      dlstbx.util.symlink.create_parent_symlink(results_directory.strpath, params['create_symlink'])

    # copy output files to result directory
    keep_ext = {
      ".xml": False,
      ".html": "log",
      ".error": "log",
      ".png": None,
      ".sh": None,
      ".ins": "result",
      ".cif": "result",
      ".hkl": "result",
      ".sca": "result",
      ".mtz": "result",
    }
    keep = {
      "fast_ep.log": "log",
      "shelxc.log": "log",
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

    xml_file = working_directory.join('fast_ep.xml')
    if xml_file.check():
      logger.info('Sending fast_ep phasing results to ISPyB')
      self.send_results_to_ispyb(xml_file.strpath)
    else:
      logger.warning(
        'Expected output file does not exist: %s' % xml_file.strpath)

    return result['exitcode'] == 0
