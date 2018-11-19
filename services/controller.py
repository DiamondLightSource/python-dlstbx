from __future__ import absolute_import, division, print_function

import dlstbx.util.jmxstats
import json
import os.path
import threading
import time

from dlstbx.zocalo.controller.strategyenvironment import StrategyEnvironment
from procrunner import run_process
from workflows.services.common_service import CommonService

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
  _sync_subscription_id = None
  _status_subscription_id = None

  # The controller should continuously check itself to ensure it does sensible
  # things.
  last_status_seen = 0
  last_self_check = 0
  last_sync_sent = 0

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
    self._transport.subscription_callback_set_intercept(self.transport_interceptor)

    # Set up ActiveMQ JMX interface
    self._jmx = dlstbx.util.jmxstats.JMXAPI()

    # Listen to service announcements to build picture of running services.
    self._status_subscription_id = str(self._transport.subscribe_broadcast(
        'transient.status',
        self.receive_status_msg,
        retroactive=True,
    ))

    # Listen to queue status reports for information on queue utilization.
    self._transport.subscribe_broadcast('transient.queue_status',
                                        self.receive_queue_status)

    # Run the main control function at least every three seconds, but only start
    # surveying after 20 seconds of listening in.
    self.last_survey = time.time() + 20
    self._register_idle(3, self.survey_operations)

  def transport_interceptor(self, callback):
    '''Override the default transport interceptor as follows:
       Incoming messages are still put on the main service queue as before,
       but if the message is from the synchronization channel then update the
       last seen timer immediately before processing the message in the main
       thread.
       The ID of the synchronization channel is kept in
       self._sync_subscription_id.
    '''
    original_interceptor = self._transport_interceptor(callback)
    def recognize_synchronization_message(header, message):
      if self._sync_subscription_id and header.get('subscription') == self._sync_subscription_id:
        # Synchronization message detected
        self.master_last_checked = time.time()
      if self._sync_subscription_id and header.get('subscription') == self._status_subscription_id:
        # Status message detected
        self.last_status_seen = time.time()
      return original_interceptor(header, message)
    return recognize_synchronization_message

  def survey_operations(self):
    '''Check the overall data processing infrastructure state and ensure that
       everything is working fine and resources are deployed appropriately.'''
    self.self_check()

    # Only run once every approx. three seconds
    if self.last_survey > time.time() - 2.95:
      return
    self.last_survey = time.time()

    self.check_for_strategy_updates()
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
      self.log.warning(
          'Could not read timestamp of controller service strategy file, %s',
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
            self._sync_subscription_id = str(self._transport.subscribe(
                'transient.controller',
                self.receive_sync_msg,
                exclusive=True,
                transformation=True,
            ))
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

    if not self.master and self.last_sync_sent + 30 > time.time():
      # Is not master, so no self-check required. Limit the number of sync messages sent.
      return

    self._transport.send('transient.controller', 'synchronization message',
                         expiration=60, persistent=False)
    self.last_sync_sent = time.time()

    # Check that synchronization messages are received
    if self.master:
      if self.master_last_checked + 60 < time.time():
        self.log.error(
          "Inconsistent status: No sync messages received over 60 seconds, " + \
          "shutting down.")
        self.master_disable()
      if self.last_status_seen + 60 < time.time():
        self.log.error(
          "Inconsistent status: No status messages received over 60 seconds, " + \
          "shutting down.")
        self.master_disable()

  def receive_status_msg(self, header, message):
    '''Process incoming status message. Acquire lock for status dictionary before updating.'''
    self.last_status_seen = time.time()

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
      instance['workflows'] = map(int, message['workflows'].split('.'))
    except Exception:
      self.log.debug('Could not parse workflows version sent by %s', instance['host'])
    try:
      instance['dlstbx'] = map(int, message['dlstbx'].split(' ', 2)[1].split('-', 1)[0].split('.', 1))
    except Exception:
      self.log.debug('Could not parse dlstbx version sent by %s', instance['host'])

    if instance['wfstatus'] == CommonService.SERVICE_STATUS_STARTING \
        and message.get('tag'):
      self._se.register_instance_tag_as_host(message['tag'], instance['host'], instance['service'])

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

    self.cleanup_and_broadcast_queue_status()
    self.survey_operations() # includes self_check()

  def receive_queue_status(self, header, message):
    if self.master:
      self.log.debug('Ignoring queue status update as master controller')
      return
    self.log.debug('Received queue status update')
    with self._lock:
      self.queue_status = message

  def cleanup_and_broadcast_queue_status(self):
    '''Regularly discard information about old queues that are no longer
       around, and notify other controller instances of status quo.'''

    # Only run once every 30 seconds
    cutoff = time.time() - 30
    if self.last_queue_status_expiration > cutoff:
      return
    self.last_queue_status_expiration = time.time()

    self.log.debug('Cleaning up and broadcasting queue status information')
    with self._lock:
      self.queue_status = { dest: data for dest, data in self.queue_status.iteritems()
                            if data['timestamp'] >= cutoff }
      self._transport.broadcast('transient.queue_status', self.queue_status)

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
    self._set_name("DLS Controller (defunct)")
    self._shutdown()

  def queue_introspection_trigger(self):
    '''Trigger ActiveMQ statistics plugin to send out queue information.
       This function also starts a timer so that it is retriggered after
       a fixed time interval.'''
    if not self.master:
      return
    retrigger = threading.Timer(4, self.queue_introspection_trigger)
    retrigger.daemon = True
    retrigger.start()
    for queue in self._se.watched_queues():
      qstat = self._jmx.org.apache.activemq(
          type="Broker",
          brokerName="localhost",
          destinationType="Queue",
          destinationName=self.namespace + '.' + queue,
          attribute=','.join(('QueueSize', 'EnqueueCount', 'DequeueCount', 'InFlightCount')),
      )
      if qstat and qstat['status'] == 200:
        report = qstat['value']
        report['timestamp'] = qstat['timestamp']
        with self._lock:
          self.queue_status[queue] = report

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
      except Exception as e:
        self.log.info('Failed to start %s with %s, error: %s', service, str(attempt), str(e), exc_info=True)
    self.log.warning('Could not start %s, all available options exhausted', service)
    return False

  def launch_cluster(self, service=None, cluster="cluster", queue="admin.q", module="dials", tag="", **kwargs):
    assert service
    result = run_process(
      [ self.service_launch_script, service ],
      environment_override={
        'CLUSTER': cluster,
        'QUEUE': queue,
        'DIALS': module,
        'TAG': tag,
      },
      timeout=15,
    )
    self.log.debug('Cluster launcher script for %s returned result: %s', service, json.dumps(result))
    # Trying to start jobs can be very time intensive, ensure the master status is not lost during balancing
    self.self_check()
    return result.get('exitcode') == 0

  def launch_testcluster(self, **kwargs):
    kwargs["cluster"] = "testcluster"
    return self.launch_cluster(**kwargs)

  def kill_service(self, instance):
    self.log.info("Shutting down instance %s (%s)", instance['host'], str(instance.get('title')))
    self._transport.broadcast('command', { 'host': instance['host'], 'command': 'shutdown' })
    return True
