from __future__ import division
import dlstbx.workflow.transport
import dlstbx.workflow.services
import multiprocessing
import Queue
import threading
import time

class Frontend():
  def __init__(self, transport=None, service=None):
    self.__lock = threading.RLock()
    self.__hostid = self._generate_unique_host_id()
    self._service = None
    self._queue_commands = None
    self._queue_frontend = None

    # Connect to the network transport layer
    if transport is None or isinstance(transport, basestring):
      self._transport = dlstbx.workflow.transport.lookup(transport)()
    else:
      self._transport = transport()
    if not self._transport.connect():
      print "Could not connect to transport layer"
      self._transport = None

    # Start broadcasting node information
    self._advertise_interval = 6 # seconds
    self._advertise_start()

    # Start service if one has been requested
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

  def _advertise_start(self):
    '''Start a background thread to regularly broadcast the current node status.'''
    with self.__lock:
      if not hasattr(self, '_advertise_lock'):
        self._advertise_lock = threading.Lock()
    with self._advertise_lock:
      self._advertise_next = 0
      if not hasattr(self, '_advertise_thread'):
        self._advertise_thread = threading.Thread(
            target=self._advertise_timer,
            name='heartbeat')
        self._advertise_thread.daemon = True
        self._advertise_thread.start()

  def _advertise_timer(self):
    '''Advertising timer thread.'''
    while self._transport is not None:
      wait_for = self._advertise()
      if wait_for is None:
        time.sleep(self._advertise_interval)
      else:
        time.sleep(wait_for)

  def _advertise(self, force=False):
    '''Advertise current frontend and service status to transport layer, and
       broadcast useful information about this node.'''
    with self._advertise_lock:
      if not force and self._advertise_next > time.time():
        return self._advertise_next - time.time()
      with self.__lock:
        if self._transport:
          self._transport.broadcast_retain('stuff', channel=self.get_host_id())
      self._advertise_next = time.time() + self._advertise_interval
      return self._advertise_interval
