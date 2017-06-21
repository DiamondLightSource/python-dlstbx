from __future__ import absolute_import, division
import json
import os.path
import threading
import time
from workflows.services.common_service import CommonService
from dials.util.procrunner import run_process
from dlstbx.zocalo.controller.strategyenvironment import StrategyEnvironment

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

  # Time of the last service allocation balancing
  last_balance = 0

  # Time of the last check for a new service strategy file
  timestamp_strategies_checked = None

  # Timestamp of the most recently loaded strategy file
  timestamp_strategies_loaded = None

  # Keep track of all queue statistics
  queue_status = {}

  # Regularly discard old queue statistics
  last_queue_status_expiration = 0

  def initializing(self):
    '''Subscribe to relevant channels and Try to take back control.'''
    self.log.info("Controller starting up")

    # Create lock to synchronize access to the internal service directory
    self._lock = threading.RLock()

    # Create a strategy environment, which is an object that does services and
    # instances bookkeeping and allocation.
    self._se = StrategyEnvironment()

    if self._environment.get('live'):
      self.strategy_file = '/dls_sw/apps/zocalo/live/strategy/controller-strategy.json'
      self.service_launch_script = '/dls_sw/apps/zocalo/live/launch_service'
      self.namespace = 'zocalo'
    else:
      self.strategy_file = '/dls_sw/apps/zocalo/controller-strategy-test.json'
      self.service_launch_script = '/dls_sw/apps/zocalo/test_launch_service'
      self.namespace = 'zocdev'

    # Listen to service announcements to build picture of running services.
    self._transport.subscribe_broadcast('transient.status',
                                        self.receive_status_msg,
                                        retroactive=True)

    # Listen to queue status reports to build picture of messages flying about.
    self._transport.subscribe_broadcast('transient.queue_status.raw',
                                        self.receive_queue_status, transformation=True)

    # Run the main control function at least every three seconds, but only start
    # surveying after 20 seconds of listening in.
    self.last_survey = time.time() + 20
    self._register_idle(3, self.survey_operations)

  def survey_operations(self):
    '''Check the overall data processing infrastructure state and ensure that
       everything is working fine and resources are deployed appropriately.'''
    self.self_check()

    # Only run once every approx. three seconds
    if self.last_survey > time.time() - 2.95:
      return
    self.last_survey = time.time()

    self.check_for_strategy_updates()

    self.log.debug('Surveying.')
    self._se.update_allocation(queue_statistics=self.queue_status)

    # New master should wait a bit before beginning to mess with services
    if not self.master: return
    if self.master_since == 0 or \
       self.master_since > time.time() - 30:
      self.log.debug('Master controller in grace period.')
      return

    # Only balance once every approx. 15 seconds
    if self.last_balance > time.time() - 15:
      return
    self.last_balance = time.time()
    self.log.debug('Balancing.')

    self._se.balance_services(callback_stop=self.kill_service, callback_start=self.start_service)

  def check_for_strategy_updates(self):
    if self.timestamp_strategies_checked > time.time() - 60:
      return
    self.timestamp_strategies_checked = time.time()

    try:
      strategies_file_timestamp = os.path.getmtime(self.strategy_file)
    except Exception:
      self.log.warn('Could not read timestamp of controller service strategy file, %s',
                    self.strategy_file)
      return

    if strategies_file_timestamp > self.timestamp_strategies_loaded:
      self.log.debug('New strategy file detected')
      with self._lock:
        try:
          with open(self.strategy_file, 'r') as fh:
            self._se.update_strategies(json.load(fh))
          self.log.info('Loaded controller service strategies from file')

          if self.timestamp_strategies_loaded is None:
            # This controller instance is now eligible to become a master.
            # Connect to a synchronization channel. This is used to determine
            # master controller status.
            self._set_name("DLS Controller (Standby)")
            self._transport.subscribe('transient.controller',
                                      self.receive_sync_msg, exclusive=True)
            self.log.debug('Controller may now become master')

          self.timestamp_strategies_loaded = strategies_file_timestamp
        except Exception:
          self.log.error('Error loading strategy file', exc_info=True)

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

    instance = {
      'dlstbx': (0, 0),
      'host': str(message.get('host',''))[:250],
      'last-seen': time.time(),
      'service': message.get('serviceclass'),
      'title': str(message.get('service'))[:100],
      'wfstatus': message.get('status'),
      'workflows': (0, 0),
    }

    if instance['wfstatus'] == CommonService.SERVICE_STATUS_NEW:
      # Message does not contain enough information to associate it with an
      # expected service instance. Ignore it and wait for the next message.
      return

    try:
      instance['workflows'] = map(int, message['workflows'].split('.', 1))
    except Exception:
      self.log.debug('Could not parse workflows version sent by %s', instance['host'])
    try:
      instance['dlstbx'] = map(int, message['dlstbx'].split(' ', 2)[1].split('-', 1)[0].split('.', 1))
    except Exception:
      self.log.debug('Could not parse dlstbx version sent by %s', instance['host'])

    if instance['wfstatus'] == CommonService.SERVICE_STATUS_STARTING \
        and message.get('tag'):
      self._se.register_instance_tag_as_host(message['tag'], instance['host'])

    if instance['wfstatus'] in \
          (CommonService.SERVICE_STATUS_STARTING,
           CommonService.SERVICE_STATUS_IDLE,
           CommonService.SERVICE_STATUS_TIMER,
           CommonService.SERVICE_STATUS_PROCESSING,
           CommonService.SERVICE_STATUS_NONE):
      self._se.update_instance(instance, set_alive=True)
    elif instance['wfstatus'] in \
          (CommonService.SERVICE_STATUS_SHUTDOWN,
           CommonService.SERVICE_STATUS_END,
           CommonService.SERVICE_STATUS_ERROR,
           CommonService.SERVICE_STATUS_TEARDOWN):
      self.log.debug('Service %s expired.', str(instance))
      self._se.update_instance(instance, set_alive=False)
    else:
      self._se.update_instance(instance)
    self.survey_operations() # includes self_check()

  def receive_sync_msg(self, header, message):
    '''When a synchronization message is received, then this instance is currently
       the master controller.'''
    self.master_last_checked = time.time()
    if not self.master:
      self.master_enable()
    self.survey_operations() # includes self_check()

  def receive_queue_status(self, header, message):
    '''Parse an incoming ActiveMQ Advisory message, which describes the status
       of one queue, and aggregate the information.'''
    if header.get('type') != 'Advisory' or not header.get('timestamp'):
      return
    report = self.parse_advisory(message.get('map', {}).get('entry', []))
    if not report:
      return
    destination = report.get('destinationName')
    if destination and destination.startswith('queue://' + self.namespace):
      destination = destination[8 + len(self.namespace) + 1:]
      report['timestamp'] = int(header['timestamp']) / 1000
      with self._lock:
        self.queue_status[destination] = report
    self.expire_queue_status()

  def expire_queue_status(self):
    '''Regularly discard information about old queues that are no longer
       around.'''
    cutoff = time.time() - 30

    # Only expire once every 30 seconds
    if self.last_queue_status_expiration > cutoff:
      return
    self.last_queue_status_expiration = time.time()

    with self._lock:
      self.queue_status = { dest: data for dest, data in self.queue_status.iteritems()
                            if data['timestamp'] >= cutoff }

  @staticmethod
  def parse_advisory(message):
    '''Convert ActiveMQ JSON Advisory to Python dictionary.'''
    report = {}
    for entry in message:
      if 'string' in entry:
        if isinstance(entry['string'], list):
          name = entry['string'].pop(0)
        else:
          name = entry['string']
          del(entry['string'])
      if len(entry) == 1:
        value_type = entry.iterkeys().next()
        report[name] = entry.itervalues().next()
        if value_type in ('long', 'int'):
          report[name] = int(report[name])
        if isinstance(report[name], list) and len(report[name]) == 1:
          report[name] = report[name][0]
      else:
        report[name] = entry
    return report

  def master_enable(self):
    '''Promote this service instance to master controller.'''
    if not self.master:
      self.master_since = time.time()
    self.master = True
    self._set_name("DLS Controller (Master)")
    self.log.info("Controller promoted to master")
    self.queue_introspection_trigger()

  def master_disable(self):
    '''Demote this service instance from master controller.'''
    self.master = False
    self.master_since = 0
    self._set_name("DLS Controller")
    self.log.info("Controller demoted")

  def queue_introspection_trigger(self):
    '''Trigger ActiveMQ statistics plugin to send out queue information.
       This function also starts a timer so that it is retriggered after
       a fixed time interval.'''
    if not self.master:
      return
    retrigger = threading.Timer(4, self.queue_introspection_trigger)
    retrigger.daemon = True
    retrigger.start()
    self.log.debug("Introspection trigger")
    self._transport.send('ActiveMQ.Statistics.Destination.' + self.namespace + '.>', '', headers = { 'JMSReplyTo': 'topic://' + self.namespace + '.transient.queue_status.raw' }, ignore_namespace=True)

  def start_service(self, instance, init):
    if not init:
      return False
    service = instance['service']
    tag = instance['tag']
    for attempt in init:
      try:
        attempt['service'] = service
        attempt['tag'] = tag
        launch_function = getattr(self, 'launch_' + attempt.get('type'))
        if launch_function(**attempt):
          self.log.info('Successfully started new instance of %s', service)
          return True
        self.log.info('Could not start %s with %s', service, str(attempt))
      except Exception, e:
        self.log.info('Failed to start %s with %s, error: %s', service, str(attempt), str(e), exc_info=True)
    self.log.warn('Could not start %s, all available options exhausted', service)
    return False

  def launch_cluster(self, service=None, cluster="cluster", queue="admin.q", module="dials", tag="", **kwargs):
    assert service
    result = run_process(
      [ self.service_launch_script, service ],
      environ={
        'CLUSTER': cluster,
        'QUEUE': queue,
        'DIALS': module,
        'TAG': tag,
      },
      timeout=15,
    )
    from pprint import pprint
    pprint(result)
    self.log.debug('Cluster launcher script for %s returned result: %s', service, json.dumps(result))
    return result.get('exitcode') == 0

  def launch_testcluster(self, **kwargs):
    kwargs["cluster"] = "testcluster"
    return self.launch_cluster(**kwargs)

  def kill_service(self, instance):
    self.log.info("Shutting down instance %s (%s)", instance['host'], str(instance.get('title')))
    self._transport.send('transient.command.' + instance['host'], { 'command': 'shutdown' })
    return True
