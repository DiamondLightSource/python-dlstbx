from __future__ import division
import dlstbx.workflow.transport
import dlstbx.workflow.services
import multiprocessing
import Queue
import threading

class Frontend():
  def __init__(self, transport=None, service=None):
    self.__lock = threading.RLock()
    self.__hostid = self._generate_unique_host_id()
    self._service = None
    self._queue_commands = None
    self._queue_frontend = None

    if transport is None or instance(transport, basestring):
      self._transport = dlstbx.workflow.transport.lookup(transport)()
    else:
      self._transport = transport()

    if service is not None:
      self.switch_service(service)

  def run(self):
    print "Current service:", self._service
    n = 20
    while n > 0:
      if self._queue_frontend is not None:
        try:
          message = self._queue_frontend.get(True, 1)
          print message
        except Queue.Empty:
          pass
      n = n - 1
    print "Fin."

  def get_host_id(self):
    '''Get a cached copy of the host id.'''
    return self.__hostid

  def _generate_unique_host_id(self):
    '''Generate a unique ID, that is somewhat guaranteed to be unique among all
       instances running at the same time.'''
    import socket
    host = '.'.join(reversed(socket.gethostname().split('.')))
    import os
    pid = os.getpid()
    return "%s.%d" % (host, pid)

  def switch_service(self, new_service):
    '''Start a new service in a subprocess.
       Service can be passed by name or class.'''
    with self.__lock:
      # Terminate existing service if necessary
      if self._service is not None:
        self._terminate_service()

      # Find service class if necessary
      if isinstance(new_service, basestring):
        service_class = dlstbx.workflow.services.lookup(new_service)
      else:
        service_class = new_service

      # Set up queues and connect new service object
      self._queue_commands = multiprocessing.Queue()
      self._queue_frontend = multiprocessing.Queue()
      service_instance = service_class(
        commands=self._queue_commands,
        frontend=self._queue_frontend)

      # Start new service in a separate process
      self._service = multiprocessing.Process(
        target=service_instance.start, args=())
      self._service.daemon = True
      self._service.start()

  def _terminate_service(self):
    '''Force termination of running service.
       Disconnect queues as they may get corrupted'''
    with self.__lock:
      self._service.terminate()
      self._service = None
      self._queue_commands = None
      self._queue_frontend = None
