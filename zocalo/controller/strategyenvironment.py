from __future__ import absolute_import, division

import dlstbx.zocalo.controller.strategy.simple
import logging
import threading
import time
import uuid

class StrategyEnvironment(object):
  # From the view of the controller strategy each service and service slot
  # (which is a place holder) is in one of the following stages:
  #   HOLD:     A slot that has been added recently. The service has not been started yet.
  #   PREPARE:  A slot that has been marked for a service start.
  #   STARTING: The service for this slot has been started, waiting for ready signal.
  #   RUNNING:  This service is running.
  #   HOLDSHDN: This service has been marked for shutdown recently.
  #   SHUTDOWN: This service is ready to be shut down
  #   EXPIRE:   This service has been shut down and will disappear soon.

  S_STATUS_CODE_RANGE = 7
  S_HOLD, S_PREPARE, S_STARTING, S_RUNNING, S_HOLDSHDN, S_SHUTDOWN, S_EXPIRE = \
      range(S_STATUS_CODE_RANGE)

  lock = threading.Lock()
  log = logging.getLogger('dlstbx.zocalo.controller.strategyenvironment')

  def __init__(self):
    self._classlist = {
        'simple': dlstbx.zocalo.controller.strategy.simple.SimpleStrategy,
    }
    self.assessments = {}
    self.environment = {
        'instances': {},
        'services': {},
    }
    self.strategies = {}

  def load_strategy(self, strategy):
    cls = self._classlist[strategy['strategy']]
    return cls(**strategy)

  def update_strategies(self, strategy_list):
    new_strategies = { svc['service']: self.load_strategy(svc) for svc in strategy_list }
    with self.lock:
      self.strategies = new_strategies

  def create_instance(self, service, status=None):
    timestamp = time.time()
    instance = {
        'service': service,
        'tag': str(uuid.uuid4()),
        'status': status if status else self.S_HOLD,
        'status-set': timestamp,
        'first-seen': timestamp,
        'last-seen': timestamp,
    }
    if service not in self.environment['services']:
      self.environment['services'][service] = {}
    self.environment['services'][service][instance['tag']] = instance
    self.environment['instances'][instance['tag']] = instance
    return instance

  def register_instance_tag_as_host(self, tag, host):
    with self.lock:
      self.log.debug("Replacing instance tag %s with host id %s", tag, host)
      instance = self.environment['instances'].get(tag)
      if instance:
        instance['host'] = host
        del self.environment['instances'][tag]
        del self.environment['services'][instance['service']][tag]
        self.environment['instances'][host] = instance
        self.environment['services'][instance['service']][host] = instance

  def update_instance(self, instance, set_alive=None):
    host = instance['host']
    with self.lock:
      if host not in self.environment['instances']:
        # New, unsolicited service
        instance['first-seen'] = instance.get('first-seen', instance.get('last-seen', time.time()))
        instance['status'] = instance.get('status', self.S_RUNNING)
        self.environment['instances'][host] = instance
        if instance['service'] not in self.environment['services']:
          self.environment['services'][instance['service']] = {}
        self.environment['services'][instance['service']][host] = self.environment['instances'][host]

      if instance['service'] != self.environment['instances'][host]['service']:
        # Update service assignment
        del self.environment['services'][self.environment['instances'][host]['service']][host]
        if instance['service'] not in self.environment['services']:
          self.environment['services'][instance['service']] = {}
        self.environment['services'][instance['service']][host] = self.environment['instances'][host]

      self.environment['instances'][host].update(instance)
      instance = self.environment['instances'][host]

      if set_alive == True and instance['status'] in (self.S_HOLD, self.S_PREPARE, self.S_STARTING):
        instance['status'] = self.S_RUNNING
      if set_alive == False:
        instance['status'] = self.S_EXPIRE

  def remove_instance(self, instance):
    if isinstance(instance, dict):
      if 'tag' in instance:
        self.remove_instance(instance['tag'])
      if 'host' in instance:
        self.remove_instance(instance['host'])
    else:
      i = self.environment['instances'].pop(instance, None)
      if i and 'service' in i:
        del self.environment['services'][i['service']][instance]

  @staticmethod
  def order_instances(instances, reverse=False):
    def fitness(i):
      return (
          tuple(i.get('workflows', (0, 0))),
          tuple(i.get('dlstbx', (0, 0))),
        )
    decorated = [(fitness(inst), i, inst) for i, inst in enumerate(instances)]
    decorated.sort(reverse=reverse)
    return [inst for fit, i, inst in decorated]

  def pick_instances(self, instances_list, to_status, max_count, highest_fitness=True):
    ordered_instances = self.order_instances(instances_list, reverse=highest_fitness)
    picked_instances = 0
    for i in ordered_instances[:max_count]:
      i['status'] = to_status
      i['status-set'] = time.time()
      picked_instances += 1
    return picked_instances

  def _allocate_service(self, service):
    instances_needed = self.strategies[service].assess(self.environment) \
                       .get('required', {}).get('count')
    if instances_needed is None:
      # Everything apparently fine as it is
      return

    existing_instances = { x: [] for x in range(self.S_STATUS_CODE_RANGE) }
    for instance in self.environment['services'][service]:
      existing_instances[self.environment['services'][service][instance]['status']].append(
          self.environment['services'][service][instance])
    count_instances = { x: len(existing_instances[x]) for x in existing_instances }

    def log_change(count, s_from, s_to):
      self.log.debug("moved %d instances of %s from %s to %s",
          count, str(service), s_from, s_to)

    gap = instances_needed
    if gap < count_instances[self.S_RUNNING]:
      # pick excess S_RUNNING to S_HOLDSHDN
      picked = self.pick_instances(existing_instances[self.S_RUNNING], self.S_HOLDSHDN,
                                   count_instances[self.S_RUNNING] - gap,
                                   highest_fitness=False)
      count_instances[self.S_RUNNING] -= picked
      count_instances[self.S_HOLDSHDN] += picked
      log_change(picked, 'RUNNING', 'HOLD_SHUTDOWN')
    gap = gap - count_instances[self.S_RUNNING] - count_instances[self.S_STARTING]

    if gap > 0 and count_instances[self.S_HOLDSHDN]:
      # pick S_HOLDSHDN instances to S_RUNNING
      picked = self.pick_instances(existing_instances[self.S_HOLDSHDN], self.S_RUNNING, gap)
      count_instances[self.S_RUNNING] += picked
      count_instances[self.S_HOLDSHDN] -= picked
      log_change(picked, 'HOLD_SHUTDOWN', 'RUNNING')
      gap = gap - picked

    if gap > 0 and count_instances[self.S_SHUTDOWN]:
      # pick S_SHUTDOWN instances to S_RUNNING
      picked = self.pick_instances(existing_instances[self.S_SHUTDOWN], self.S_RUNNING, gap)
      count_instances[self.S_RUNNING] += picked
      count_instances[self.S_SHUTDOWN] -= picked
      log_change(picked, 'SHUTDOWN', 'RUNNING')
      gap = gap - picked

    if count_instances[self.S_PREPARE]:
      # keep up to gap instances in S_PREPARE, discard rest
      if gap <= 0:
        selected_for_removal = existing_instances[self.S_PREPARE]
      else:
        selected_for_removal = self.order_instances(existing_instances[self.S_PREPARE], reverse=True)[gap:]
      for i in selected_for_removal:
        self.remove_instance(i)
        count_instances[self.S_PREPARE] -= 1
      if selected_for_removal:
        log_change(len(selected_for_removal), 'PREPARE', '/dev/null')
      # then
      gap = gap - count_instances[self.S_PREPARE]

    selected_for_removal = None
    if gap <= 0 and count_instances[self.S_HOLD]:
      # discard all S_HOLD instances
      selected_for_removal = existing_instances[self.S_HOLD]
    elif gap < count_instances[self.S_HOLD]:
      # keep up to $gap HOLD instances
      selected_for_removal = self.order_instances(existing_instances[self.S_HOLD], reverse=True)[gap:]
    elif gap > count_instances[self.S_HOLD]:
      # add new HOLD instances
      log_change(gap - count_instances[self.S_HOLD], '/dev/null', 'HOLD')
      for i in xrange(gap - count_instances[self.S_HOLD]):
        instance = self.create_instance(service)
        count_instances[self.S_HOLD] += 1
    if selected_for_removal:
      for i in selected_for_removal:
        self.remove_instance(i)
        count_instances[self.S_HOLD] -= 1
      log_change(len(selected_for_removal), 'HOLD', '/dev/null')

  def update_allocation(self):
    '''Check with each registered strategy whether any changes are required.'''
    def log_change(instance, s_from, s_to):
      self.log.debug("timer event: moved %s instance %s from %s to %s",
          str(instance.get('service', '???')), str(instance.get('host', '(unassigned)')), s_from, s_to)

    with self.lock:
      for service in self.strategies:
        if service not in self.environment['services']:
          self.environment['services'][service] = {}
        self._allocate_service(service)

      # Advance timers
      current_timestamp = time.time()
      expiration_time = 30
      hold_time = 30
      discard_instances = []
      for key, instance in self.environment['instances'].iteritems():
        if 'status-set' not in instance:
          instance['status-set'] = time.time()
        if instance['status'] == self.S_HOLD and instance['status-set'] + hold_time < current_timestamp:
          instance['status'] = self.S_PREPARE
          instance['status-set'] = time.time()
          log_change(instance, 'HOLD', 'PREPARE')
        if instance['status'] == self.S_HOLDSHDN and instance['status-set'] + hold_time < instance['last-seen']:
          instance['status'] = self.S_SHUTDOWN
          instance['status-set'] = time.time()
          log_change(instance, 'HOLD_SHUTDOWN', 'SHUTDOWN')
        if instance['status'] not in (self.S_HOLD, self.S_PREPARE, self.S_EXPIRE) \
            and instance['last-seen'] < current_timestamp - expiration_time:
          log_change(instance, 'running state (%d)' % instance['status'], 'EXPIRE')
          instance['status'] = self.S_EXPIRE
          instance['status-set'] = time.time()
        if instance['status'] == self.S_EXPIRE and instance['status-set'] < current_timestamp - expiration_time:
          discard_instances.append(key)
          log_change(instance, 'EXPIRE', '/dev/null')

      for instance in discard_instances:
        self.remove_instance(instance)

  def balance_services(self, callback_start=None, callback_stop=None):
    available_for_reassignment = set()
    shutdown = set()
    startup = set()
    reassign = {}
    with self.lock:
      for key, instance in self.environment['instances'].iteritems():
        if instance['status'] == self.S_PREPARE:
          startup.add(key)
        if instance['status'] == self.S_SHUTDOWN:
          shutdown.add(key)
          available_for_reassignment.add(key)
        if instance['status'] == self.S_RUNNING and instance['service'] is None:
          available_for_reassignment.add(key)
      if startup:
        self.log.debug("Considering start of %s", str(startup))
      if shutdown:
        self.log.debug("Considering shutdown of %s", str(shutdown))
      if available_for_reassignment:
        self.log.debug("Instances available for assignment: %s", str(available_for_reassignment))

      if shutdown and callback_stop:
        for key in shutdown:
          if callback_stop(self.environment['instances'][key]):
            self.environment['instances'][key]['status'] = self.S_EXPIRE
            self.environment['instances'][key]['status-set'] = time.time()
          else:
            self.log.debug('Shutdown of %s denied', key)

      if startup and callback_start:
        for key in startup:
          if callback_start(self.environment['instances'][key]):
            self.environment['instances'][key]['status'] = self.S_STARTING
            self.environment['instances'][key]['status-set'] = time.time()
            self.environment['instances'][key]['last-seen'] = self.environment['instances'][key]['status-set']
          else:
            self.log.debug('Start of %s denied', key)
