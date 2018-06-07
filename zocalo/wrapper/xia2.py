from __future__ import absolute_import, division, print_function

import glob
import logging
import os
import shutil

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
    logger.debug("Reading xia2 results")
    from xia2.command_line.ispyb_json import ispyb_object

    message = ispyb_object()
    # Do not accept log entries from the object, we add those separately
    message['AutoProcProgramContainer']['AutoProcProgramAttachment'] = filter(
       lambda x: x.get('fileType') != 'Log', message['AutoProcProgramContainer']['AutoProcProgramAttachment'])

    source = os.path.join(os.getcwd(), 'xia2.txt')

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

    dcid = self.recwrap.recipe_step['job_parameters'].get('dcid')
    assert dcid, "No data collection ID specified."
    dcid = int(dcid)
    assert dcid > 0, "Invalid data collection ID given."
    logger.debug("Writing to data collection ID %s", str(dcid))
    for container in message['AutoProcScalingContainer']['AutoProcIntegrationContainer']:
      container['AutoProcIntegration']['dataCollectionId'] = dcid

    # Use existing AutoProcProgramID
    if self.recwrap.environment.get('ispyb_autoprocprogram_id'):
      message['AutoProcProgramContainer']['AutoProcProgram'] = \
        self.recwrap.environment['ispyb_autoprocprogram_id']

    logger.debug("Sending %s", str(message))
    self.recwrap.transport.send('ispyb', message)

    logger.info("Processing information from %s attached to data collection %s", source, str(dcid))

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']
    command = self.construct_commandline(params)

    # run xia2 in working directory

    cwd = os.path.abspath(os.curdir)

    working_directory = params['working_directory']
    if not os.path.exists(working_directory):
      os.makedirs(working_directory)
    os.chdir(working_directory)

    result = procrunner.run_process(
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

    # copy output files to result directory

    results_directory = params['results_directory']
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

    if result['exitcode'] and not os.path.isfile(os.path.join(results_directory, 'xia2.error')):
      with open(os.path.join(results_directory, 'xia2.error'), 'w') as fh:
        fh.write("(stdout/stderr output obtained by xia2 wrapper)\n\n")
        fh.write(result['stdout'])
        fh.write("\n\n----- ^^ STDOUT ^^ --- vv STDERR vv -----\n\n")
        fh.write(result['stderr'])
      allfiles.append(os.path.join(results_directory, 'xia2.error'))

    # Send results to various listeners

    os.chdir(results_directory)

    if params.get('results_symlink'):
      path_elements = results_directory.split(os.sep)

      # Full path to the symbolic link
      link_path = os.sep.join(path_elements[:-2] + [params['results_symlink']])

      # Only write symbolic link if a symbolic link is created or overwritten
      # Do not overwrite real files, do not touch real directories
      if not os.path.exists(link_path) or os.path.islink(link_path):
        # because symlink can't be overwritten, create a temporary symlink in the child directory
        # and then rename on top of potentially existing one in the parent directory.
        tmp_link = os.sep.join(path_elements[:-1] + ['.tmp.' + params['results_symlink']])
        os.symlink(os.sep.join(path_elements[-2:]), tmp_link)
        os.rename(tmp_link, link_path)

    if not result['exitcode'] and not os.path.isfile('xia2.error') and os.path.exists('xia2.json'):
      self.send_results_to_ispyb()

    logfiles = [ 'xia2.html', 'xia2.error' ]
    for result_file in filter(os.path.isfile, logfiles):
      self.record_result_individual_file({
        'file_path': results_directory,
        'file_name': os.path.basename(result_file),
        'file_type': 'log',
      })

    datafiles_path = os.path.join(results_directory, 'DataFiles')
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
