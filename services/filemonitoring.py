from __future__ import absolute_import, division
from workflows.services.common_service import CommonService

class DLSFileMonitoring(CommonService):
  '''A service that watches a directory and waits for files to appear.'''

  # Human readable service name
  _service_name = "DLS File monitoring service"

  def initializing(self):
    '''Subscribe to a channel.'''
    self._transport.subscribe('transient.file_monitor', self.monitor_directory)

  def monitor_directory(self, header, message):
    '''Monitor a directory.'''
    print "=== Monitoring ==="
    print message
    filename = 'collection_00001.cbf'
    import time
    time.sleep(5)
    print "=== I guess a file has appeared ==="
    self._transport.send('transient.file_appeared', filename)
