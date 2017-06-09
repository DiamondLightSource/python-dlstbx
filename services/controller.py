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

  strategy_file = '/dls_sw/apps/zocalo/controller-strategy.json'

  # Dictionary of all known services
  service_list = {}

  # Considered actions
  actions = {}

  def initializing(self):
    '''Subscribe to relevant channels and Try to take back control.'''
    self.log.info("Controller starting up")

    # Create lock to synchronize access to the internal service directory
    self._lock = threading.RLock()

    # Create a strategy environment, which is an object that does services and
    # instances bookkeeping and allocation.
    self._se = StrategyEnvironment()

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

    # Only run once every approx. three seconds
    if self.last_survey > time.time() - 2.95:
      return
    self.last_survey = time.time()

    self.check_for_strategy_updates()

    self.log.debug('Surveying.')
    self._se.update_allocation()

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

    self._se.balance_services(callback_stop=self.kill_service, callback_start=self.start_service_cluster)

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

  def on_expiration(self, service):
    self.log.info('Service %s expired (%s)', service['service'], str(service))

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

  def start_service_cluster(self, instance):
    service = instance['service']
    result = run_process(['/dls_sw/apps/zocalo/start_service', service], timeout=15)
    from pprint import pprint
    pprint(result)
    self.log.info('Started %s with result: %s', service, json.dumps(result))
    return result.get('exitcode') == 0

  def start_service_konsole(self, instance):
    self.log.info('Starting %s on konsole', instance['service'])
    with open('/dls/tmp/wra62962/interactrunner', 'a') as fh:
      fh.write('date\n')
      fh.write('konsole -e /home/wra62962/dials/start_service %s %s\n' % (instance['service'], instance['tag']))
    return True

  def kill_service(self, instance):
    self.log.info("Shutting down instance %s (%s)", instance['host'], str(instance.get('title')))
    self._transport.send('transient.command.' + instance['host'], { 'command': 'shutdown' })
    return True
