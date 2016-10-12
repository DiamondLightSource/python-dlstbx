from __future__ import absolute_import, division
import os
import random
import time
from workflows.services.common_service import CommonService

class DLSFileMonitoring(CommonService):
  '''A service that watches a directory and waits for files to appear.'''

  # Human readable service name
  _service_name = "DLS File monitoring service"

  def initializing(self):
    '''Subscribe to a channel.'''
    self._transport.subscribe('transient.file_monitor', self.wait_for_list_of_files)

  def wait_for_list_of_files(self, header, message):
    '''Monitor a list of files to appear in order.'''

    self._basespeed=0.1
    self._waitlimit=2048
    # This is a bug in the message handling API. This function should receive the deserialized message.
    import json
    message = json.loads(message)

    files = iter(message['files'])
    firstfile = True
    try:
      while True:
        nextfile = next(files)
        backoff = self._basespeed
        total_wait = 0
        while not os.path.exists(nextfile):
          if backoff >= self._waitlimit * 2:
            print "File", nextfile, "did not appear. Giving up."
            return
          if backoff > self._waitlimit:
            waittime = self._waitlimit
          else:
            waittime = random.uniform(self._basespeed, backoff)
          if firstfile:
            waittime = min(3, waittime)
          time.sleep(waittime)
          total_wait += waittime
          if firstfile:
            backoff += 40
          else:
            backoff *= 2
        firstfile = False
        print "File", nextfile, "appeared."
        self._transport.send('transient.destination', 'File %s appeared' % nextfile)

        # send off file to PIA
        self._transport.send('per_image_analysis', { 'file': nextfile } )
    except StopIteration:
      print "All done."
      return
