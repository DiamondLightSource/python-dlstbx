import sys
import threading
import time
import traceback

class StatusAdvertise():
  def __init__(self, interval=60, status_callback=None, transport=None):
    self._advertise_lock = threading.Lock()
    self._interval = interval
    self._status_function = status_callback
    self._transport = transport
    self._background_thread = threading.Thread(
        target=self._timer,
        name='heartbeat')
    self._background_thread.daemon = True
    self._shutdown = False

  def start(self):
    '''Start a background thread to broadcast the current node status at
       regular intervals.'''
    self._background_thread.start()

  def stop(self):
    '''Stop the background thread.'''
    self._shutdown = True

  def _timer(self):
    '''Advertise current frontend and service status to transport layer, and
       broadcast useful information about this node.
       This runs in a separate thread.'''
    while not self._shutdown:
      waitperiod = self._interval + time.time()

      try:
        with self._advertise_lock:
          status = None
          if self._status_function is not None:
            status = self._status_function()
          if self._transport is not None:
            if status is None:
              self._transport.broadcast_status()
            else:
              self._transport.broadcast_status(status)
      except Exception, e:
        # should pass these to a logging function
        print "Exception in status thread:"
        print '-'*60
        traceback.print_exc(file=sys.stdout)
        print '-'*60

      waitperiod = waitperiod - time.time()
      time.sleep(waitperiod)
