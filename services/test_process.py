from __future__ import absolute_import, division
from workflows.services.common_service import CommonService
from dials.util.procrunner import run_process

class DLSRunProcess(CommonService):
  '''A test to see if we can run another process.'''

  # Human readable service name
  _service_name = "DLS Run Process Test"

  def initializing(self):
    '''Run a process.'''
    print run_process(['/dls/tmp/wra62962/directories/runprocess/test.sh'], timeout=20)
