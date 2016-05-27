from __future__ import division
import dlstbx.workflow.transport.stomp
import multiprocessing

class Frontend():
  def __init__(self, transport=None):
    if transport is None:
      transport = dlstbx.workflow.transport.stomp.Transport()
    self._reset()

  def _reset(self):
    self._service = None
    self._queue_command = multiprocessing.Queue()
    self._queue_frontend = multiprocessing.Queue()

  def switch_service(self, new_service):
    if self._service is not None:
      self.terminate_service()

  def terminate_service(self):
    pass
