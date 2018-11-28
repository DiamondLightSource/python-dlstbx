from __future__ import absolute_import, division, print_function

import logging
import os
import py
from datetime import datetime

import dlstbx.util.symlink
import dlstbx.zocalo.wrapper
import procrunner

logger = logging.getLogger('dlstbx.wrap.big_ep')

class BigEPWrapper(dlstbx.zocalo.wrapper.BaseWrapper):

  def construct_commandline(self, params):
    '''Construct big_ep command line.
       Takes job parameter dictionary, returns array.'''

    command = ['sh', '/dls_sw/apps/mx-scripts/auto-big-ep/zoc-bigep.sh',
               params['big_ep']['autoproc_id'],
               '%4d%02d%02d_%02d%02d%02d' % tuple(datetime.now().timetuple()[:6]),
               params['big_ep']['beamline']]

    return command

  def run(self):
    assert hasattr(self, 'recwrap'), \
      "No recipewrapper object found"

    params = self.recwrap.recipe_step['job_parameters']

    from dlstbx.ispybtbx import ispybtbx
    ispyb_conn = ispybtbx()
    proposal_code, proposal_number, visit_number \
      = ispyb_conn.get_visit_name_from_dcid(params['dcid'])
    if proposal_code in ('lb', 'in', 'sw'):
      logger.info('Skipping big_ep for %s visit', proposal_code)
      return

    working_directory = py.path.local(params['working_directory'])
    results_directory = py.path.local(params['results_directory'])

    # Create working directory with symbolic link
    working_directory.ensure(dir=True)
    if params.get('create_symlink'):
      dlstbx.util.symlink.create_parent_symlink(working_directory.strpath, params['create_symlink'])

    command = self.construct_commandline(params)
    result = procrunner.run(
        command, timeout=params.get('timeout'),
        print_stdout=True, print_stderr=True,
        working_directory=working_directory.strpath,
    )

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

    # XXX what files do we want to keep?

    return result['exitcode'] == 0
