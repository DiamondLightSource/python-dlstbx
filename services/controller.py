from __future__ import absolute_import, division
import threading
import time
from workflows.services.common_service import CommonService
from dials.util.procrunner import run_process

class DLSController(CommonService):
  '''A service to supervise other services, start new instances and shut down
     existing ones depending on policy and demand.'''

  _service_name = "DLS Controller"
  _logger_name = 'dlstbx.services.controller'

  # Only one controller service at a time will instruct other services.
  # That instance of the controller service has master==true.
  master = False
  master_last_checked = 0
  master_since = 0

  # The controller should continuously check itself to ensure it does sensible
  # things.
  last_self_check = 0

  # Time of the last operations survey
  last_survey = 0

  # Dictionary of all known services
  service_list = {}

  # Considered actions
  actions = {}

  def initializing(self):
    '''Subscribe to relevant channels and Try to take back control.'''
    self.log.info("Controller starting up")

    # Create lock to synchronize access to the internal service directory
    self._lock = threading.RLock()

    # Connect to a synchronization channel. This is used to determine master
    # controller status.
    self._transport.subscribe('transient.controller', self.receive_sync_msg,
                              exclusive=True)

    # Listen to service announcements to build picture of running services.
    self._transport.subscribe_broadcast('transient.status',
                                        self.receive_status_msg,
                                        retroactive=True)

    # Run the main control function at least every three seconds
    self._register_idle(3, self.survey_operations)

  def survey_operations(self):
    '''Check the overall data processing infrastructure state and ensure that
       everything is working fine and resources are deployed appropriately.'''
    self.self_check()
    if not self.master: return

    # Only run once every approx. three seconds
    if self.last_survey > time.time() - 2.95:
      return
    self.last_survey = time.time()

    # Don't survey when the service list can't be trusted
    if self.master_since == 0 or \
       self.master_since > time.time() - 20:
      self.log.debug('Controller is too young to survey')
      return

    self.log.debug('Surveying...')
    print ""

    with self._lock:
      now = time.time()
      hosts = list(self.service_list)
      for h in hosts:
        age = (now - int(self.service_list[h]['last_seen'] / 1000))
        if age > 90:
          self.on_expiration(self.service_list[h])
          del(self.service_list[h])

      list_by_service = {}
      for s in self.service_list:
        svc_name = self.service_list[s]['service']
        if svc_name not in list_by_service:
          list_by_service[svc_name] = []
        list_by_service[svc_name].append(self.service_list[s])

    for svc in list_by_service:
      print "%dx %s" % (len(list_by_service[svc]), svc)

    expected_service = { # 'Message Consumer': { 'name': 'SampleConsumer', 'count': 1 },
                         'DLS Schlockmeister': { 'name': 'DLSSchlockMeister', 'count': 1, 'limit': 3 },
                         'DLS Filewatcher':    { 'name': 'DLSFileWatcher', 'count': 1, 'limit': 3 },
                         'DLS Dispatcher':     { 'name': 'DLSDispatcher', 'count': 1, 'limit': 3 },
                         'DLS Per-Image-Analysis': { 'name': 'DLSPerImageAnalysis', 'count': 1 },
                         'DLS cluster submitter': { 'name': 'DLSClusterSubmission', 'count': 1, 'limit': 3 }
                       }
    for service in expected_service:
      if len(list_by_service.get(service, [])) < expected_service[service].get('count', 0):
        self.consider('start', expected_service[service]['name'])
      if expected_service[service].get('limit') and len(list_by_service.get(service, [])) > expected_service[service].get('limit'):
        self.consider('stop', expected_service[service]['name'], candidates=list_by_service[service])

  def on_expiration(self, service):
    self.log.info('Service %s expired (%s)', service['service'], str(service))

  def consider(self, action, service, candidates=None):
    with self._lock:
      if (action, service) not in self.actions:
        self.log.info('Running action %s on %s', action, service)
        if action == 'start':
          self.actions[(action, service)] = time.time() + 180
          result = run_process(['/dls/tmp/wra62962/zocalo/start_service', service], timeout=15)
        elif action == 'stop':
          self.actions[(action, service)] = time.time() + 100
          candidate = candidates[0] # simplest strategy: choose first.
          self.log.debug("Should stop %s at %s", service, str(candidate))
          self.kill_service(candidate['host'])
      else:
        if self.actions[(action, service)] > time.time():
          self.log.info('Still waiting for %s on %s', action, service)
        else:
          del(self.actions[(action, service)])
          self.log.info('Giving up on %s on %s', action, service)

  def self_check(self):
    '''Check that the controller service status is consistent.'''

    # Only check once every 5 seconds
    if self.last_self_check > time.time() - 5:
      return
    self.last_self_check = time.time()

    # Send a message to the synchronization channel
    self._transport.send('transient.controller', 'synchronization message')

    # Check that synchronization messages are received
    if self.master and (self.master_last_checked + 30 < time.time()):
      self.log.warn("Inconsistent status: No sync messages received over 30 seconds, " + \
                    "relinquishing master status")
      self.master_disable()

  def receive_status_msg(self, header, message):
    '''Process incoming status message. Acquire lock for status dictionary before updating.'''
    with self._lock:
      if message['host'] not in self.service_list or \
          int(header['timestamp']) >= self.service_list[message['host']]['last_seen']:
        self.service_list[message['host']] = message
        self.service_list[message['host']]['last_seen'] = int(header['timestamp'])
    self.survey_operations() # includes self_check()

  def receive_sync_msg(self, header, message):
    '''When a synchronization message is received, then this instance is currently
       the master controller.'''
    self.master_last_checked = time.time()
    if not self.master:
      self.master_enable()
    self.survey_operations() # includes self_check()

  def master_enable(self):
    '''Promote this service instance to master controller.'''
    if not self.master:
      self.master_since = time.time()
    self.actions = {}
    self.master = True
    self._set_name("DLS Controller (Master)")
    self.log.info("Controller promoted to master")

  def master_disable(self):
    '''Demote this service instance from master controller.'''
    self.master = False
    self.master_since = 0
    self.actions = {}
    self._set_name("DLS Controller")
    self.log.info("Controller demoted")

  def kill_service(self, service_id):
    self.log.info("Sending kill signal to %s", service_id)
    self._transport.send('transient.command.' + service_id,
                         { 'command': 'shutdown' })
