from __future__ import absolute_import, division
from workflows.services.common_service import CommonService

class DLSFileMonitoringJob(CommonService):
  '''A service that creates a job for the file monitoring service'''

  # Human readable service name
  _service_name = "DLS File monitoring job generator"

  def initializing(self):
    '''Subscribe to a channel.'''
    list_of_files = \
      [ "/dls/mx-scratch/dials/example_data/wide_rotation/X4_wide_M1S4_1_%04d.cbf" % x for x in range(1, 91) ]
    self._transport.send('transient.file_monitor', { 'files': list_of_files })
    print "Job created."
