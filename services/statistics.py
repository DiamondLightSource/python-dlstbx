from __future__ import absolute_import, division
from dials.util.procrunner import run_process
from dlstbx.util.rrdtool import RRDTool
import errno
import os
import Queue
import time
from workflows.services.common_service import CommonService

class DLSStatistics(CommonService):
  '''A service to gather report statistics on and around zocalo.'''

  # Human readable service name
  _service_name = "DLS Statistics"

  # Logger name
  _logger_name = 'dlstbx.services.statistics'

  def initializing(self):
    '''Subscribe to the cluster submission queue.
       Received messages must be acknowledged.'''
    self.log.info("Statistics service starting")
    if self._environment.get('live'):
      self.rrd = RRDTool('/dls_sw/apps/zocalo/statistics')
    else:
      self.rrd = RRDTool('.')

    self.unlocked_write_from = 0
    self.unlocked_write_to = 0
    self.newly_unlocked = None
    self.queue = Queue.PriorityQueue()
    self._register_idle(5, self.process_statistics)
    self._transport.subscribe('cluster.statistics',
                              self.cluster_statistic,
                              acknowledgement=True, exclusive=True)
#    self._transport.subscribe('statistics.cluster',
#                              self.cluster_statistic,
#                              acknowledgement=True, exclusive=True)

  def cluster_statistic(self, header, message):
    '''Receive an interesting statistic about the cluster.'''
    if not isinstance(message, dict) or 'timestamp' not in message:
      # Invalid message
      self._transport.nack(header)
      return
    self.queue.put((message['timestamp'], 'cluster', header, message))
    if self.unlocked_write_from <= time.time() <= self.unlocked_write_to:
      self.unlocked_write_to = time.time() + 90
      self.process_statistics()
    elif self.unlocked_write_from < time.time():
      self.log.debug('Unlocking statistics writing one minute from now')
      self.newly_unlocked = True
      self.unlocked_write_from = time.time() + 60
      self.unlocked_write_to = time.time() + 150

  def process_statistics(self):
    '''Process some stored data if possible.'''
    if self.queue.empty():
      return
    if self.unlocked_write_from > time.time():
      return
    if self.newly_unlocked:
      self.create_all_recordfiles()
      self.newly_unlocked = False
    if time.time() > self.unlocked_write_to:
      self.log.debug('Locking statistics writing, processing all remaining data')
      while not self.queue.empty():
        self.write_out_records()
    else:
      self.write_out_records()

  def write_out_records(self):
    '''Gather a limited number of statistics and write them to the database.'''

    # Conditionally acknowledge receipt of messages
    txn = self._transport.transaction_begin()
    records = {}
    try:
      while True:
        record = self.queue.get_nowait()
        if record[1] not in records:
          records[record[1]] = []
        records[record[1]].append((record[2], record[3]))
        if len(records[record[1]]) >= 20:
          break
    except Queue.Empty:
      pass

    if not records:
      self.log.warn('Statistics service in invalid state')
      return

    if 'cluster' in records:
      headers, messages = zip(*records['cluster'])
      self.write_out_cluster_statistics(messages)
      for header in headers:
        self._transport.ack(header, transaction=txn)

    self._transport.transaction_commit(txn)
    self.log.debug("Processed %d records", sum(len(r) for r in records.itervalues()))

  def write_out_cluster_statistics(self, stats):
    records = map( lambda r:
                     "{timestampint}:{slots[general][total]}:{slots[general][broken]}:{slots[general][used-high]}:{slots[general][used-medium]}:{slots[general][used-low]}".format(
                       timestampint=int(r['timestamp']), **r),
                   stats )
    return self.rrd.update('cluster-utilization-live-general.rrd', records)

  def create_all_recordfiles(self):
    self.log.debug('Creating record files')
    if not self.rrd.create_if_required(
          'cluster-utilization-live-general.rrd', [ '--step', '60' ]
        + [ 'DS:%s:GAUGE:180:0:U' % name for name in ('slot-total', 'slot-broken', 'slot-used-h', 'slot-used-m', 'slot-used-l') ]
        + [ 'RRA:%s:0.5:1:1440' % cls for cls in ('AVERAGE', 'MAX', 'MIN') ]
        + [ 'RRA:%s:0.5:6:3360' % cls for cls in ('AVERAGE', 'MAX', 'MIN') ]
        ):
      self.log.warning('Failed to create cluster-utilization-live-general record file')
