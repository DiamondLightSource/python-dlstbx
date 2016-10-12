from __future__ import absolute_import, division
import time
from workflows.services.common_service import CommonService

class DLSFileMonitoringJob(CommonService):
  '''A service that creates a job for the file monitoring service'''

  # Human readable service name
  _service_name = "DLS File monitoring job generator"

  def initializing(self):
    '''Create a new job every 5 minutes.'''
    self.generate_job()
    self._register_idle(5 * 60, self.generate_job)

  def generate_job(self):
    '''Generate a new monitoring job.'''
    list_of_files = \
      [ "/dls/mx-scratch/dials/example_data/wide_rotation/X4_wide_M1S4_1_%04d.cbf" % x for x in range(1, 91) ]
    self._transport.send('transient.file_monitor', { 'files': list_of_files })
    print "Job created at", time.ctime()
