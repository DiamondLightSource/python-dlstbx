from __future__ import absolute_import, division, print_function

import json
import os.path
import threading
import time

from workflows.services.common_service import CommonService

class DLSXRayCentering(CommonService):
  '''A service to aggregate per-image-analysis results and identify an X-ray
     centering solution for a data collection.'''

  _service_name = "DLS X-Ray Centering"
  _logger_name = 'dlstbx.services.xray-centering'

  def initializing(self):
    '''Try to exclusively subscribe to the x-ray centering queue.'''
    self.log.info("X-Ray centering service starting up")

    self._centering_data = {}
    self._centering_lock = threading.Lock()

    self._register_idle(60, self.garbage_collect)
#   need to make this a recipe subscription
    self._transport.subscribe(
#   need to tell AMQ server to send unlimited pending messages to this queue
        'xray-centering',
        self.add_pia_result,
        exclusive=True,
    )

  def garbage_collect(self):
    '''Throw away partial scan results after a while.'''
    self.log.debug('Garbage collect')

    with self._centering_lock:
    # for dcid in centering_data:
    #   if last_seen > 15 minutes:
    #     transaction.start()
    #     ack_all_messages()
    #     send_something_to_abort_output()
    #     transaction.commit()
    #     remove dcid
    #     # could do this partially outside of lock, but probably does not matter
      pass

#   need to make this a recipe subscription
  def add_pia_result(self, header, message):
    '''Process incoming PIA result. Acquire lock for centering results dictionary before updating.'''
    self.log.debug('Received PIA result')
    # log message should include image number, count, total
    self.last_status_seen = time.time()

    with self._centering_lock:
      self._centering_data['thing'] = (header, message)
#     update last_seen time for dcid
#     if all_results_there:
#       do_centering_thing()
#       transaction.start()
#       ack_all_messages()
#       send_result_to_default+success_output()
#       transaction.commit()
#       remove dcid

#   if last_garbage_collection > 60 seconds:
#     self.garbage_collect()
