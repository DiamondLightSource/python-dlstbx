#
# dlstbx.queue_monitor
#   Monitor queue utilization
#
from __future__ import absolute_import, division, print_function

import copy
import curses
import locale
import math
import re
import sys
import threading
import time
from optparse import SUPPRESS_HELP, OptionParser

import dlstbx.util.jmxstats
jmx = dlstbx.util.jmxstats.JMXAPI()

locale.setlocale(locale.LC_ALL, '')

class QueueStatus():
  '''Monitor ActiveMQ queue activity.'''

  # Dictionary of all known queues
  last_gather = 0
  gather_interval = 5
  status = {}
  lock = threading.Lock()

  def run(self):
    '''A wrapper for the real _run() function to cleanly enable/disable the
       curses environment.'''
    curses.wrapper(self._run)

  def gather(self):
    self.last_gather = time.time()
    queues = jmx.org.apache.activemq(type="Broker", brokerName="localhost", destinationType="Queue", destinationName="*")
    if 'value' in queues:
      for destination in queues['value']:
        queues['value'][destination]
        dest = destination[destination.index('destinationName='):]
        dest = (dest.split(',')[0])[16:]
        self.process_report(queues['value'][destination], destination, dest)
    topics = jmx.org.apache.activemq(type="Broker", brokerName="localhost", destinationType="Topic", destinationName="*")
    if 'value' in topics:
      for destination in topics['value']:
        topics['value'][destination]
        dest = destination[destination.index('destinationName='):]
        dest = (dest.split(',')[0])[16:]
        self.process_report(topics['value'][destination], destination, dest)

  @staticmethod
  def formatnumber(stdscr, number):
    stdscr.addstr(str(number))

  def _run(self, stdscr):
    '''Main display function'''
    curses.use_default_colors()
    curses.curs_set(False)
    curses.init_pair(1, curses.COLOR_RED, -1)
    curses.init_pair(2, curses.COLOR_BLACK, -1)
    curses.init_pair(3, curses.COLOR_GREEN, -1)
    curses.init_pair(4, curses.COLOR_YELLOW, -1)
    curses.init_pair(5, curses.COLOR_BLUE, -1)
    curs_y, curs_x = stdscr.getmaxyx()

    def arrow(change):
      if change >= 3:
        stdscr.addstr(u'\u2b08'.encode('utf-8'), curses.color_pair(3) + curses.A_BOLD)
      elif change >= 2:
        stdscr.addstr(u'\u2197'.encode('utf-8'), curses.color_pair(3) + curses.A_BOLD)
      elif change >= 1:
        stdscr.addstr(u'\u2197'.encode('utf-8'), curses.color_pair(3))
      elif change <= -3:
        stdscr.addstr(u'\u2b0a'.encode('utf-8'), curses.color_pair(1) + curses.A_BOLD)
      elif change <= -2:
        stdscr.addstr(u'\u2198'.encode('utf-8'), curses.color_pair(1) + curses.A_BOLD)
      elif change <= -1:
        stdscr.addstr(u'\u2198'.encode('utf-8'), curses.color_pair(1))

    try:
      while True:
        if self.last_gather + self.gather_interval < time.time():
          self.gather()

      # Check if screen was re-sized (True or False)
      resize = curses.is_term_resized(curs_y, curs_x)

        # Redraw in new layout if terminal window has been resized
        stdscr.clear()
        stdscr.scrollok(True)
        if resize is True:
          curs_y, curs_x = stdscr.getmaxyx()
          curses.resizeterm(curs_y, curs_x)
        rows = curses.LINES

        # Expire old destinations, select destinations to show
        with self.lock:
          destinations = self.status.keys()
          self.status = {k: self.status[k] for k in destinations if self.status[k].get('last-seen', 0) + (self.gather_interval * 3) >= time.time()}
          status_list = self.status.values()
        status_list = filter(lambda s:s['relevance'] > 0, status_list)
        status_list.sort(key=lambda s:s['relevance'], reverse=True)

        stdscr.addstr(0, 0, 'DLS Zocalo queue monitor', curses.color_pair(4))
        if curses.COLS > 55:
          stdscr.addstr(0, 30, 'queues:', curses.color_pair(4))
          stdscr.addstr(0, 38, str(len(status_list)), curses.color_pair(3))
        if curses.COLS > 67:
          stdscr.addstr(0, 47, 'messages:', curses.color_pair(4))
          stdscr.addstr(0, 57, str(sum(s['size'] for s in status_list)), curses.color_pair(3))
#       stdscr.addstr(0, 0, 'DLS', curses.A_BOLD + curses.color_pair(4))

        reserved_rows = 1
        status_list = copy.deepcopy(status_list[:(rows - reserved_rows)])
        if not status_list:
          stdscr.refresh()
          time.sleep(0.2)
          continue

        for s in status_list:
          if s['destinationName'].startswith('topic://'):
            s['shortdestlen'] = len(s['shortdest']) + 1
            s['shortdest'] = s['shortdest'].encode('utf-8') + u'\u29d3'.encode('utf-8')

        longestname = max(s.get('shortdestlen', len(s['shortdest'])) for s in status_list)
        for n, s in enumerate(status_list):
          stdscr.move(reserved_rows + n, longestname - s.get('shortdestlen', len(s['shortdest'])))
          stdscr.addstr(s['shortdest'])

        col = longestname + 1
        longestenq = max(len(str(s.get('change-enqueueCount', 0))) for s in status_list)
        for n, s in enumerate(status_list):
          stdscr.move(reserved_rows + n, col + longestenq - len(str(s.get('change-enqueueCount', 0))))
          enqueued = s.get('change-enqueueCount')
          if enqueued > 0:
            stdscr.addstr(str(enqueued), curses.color_pair(3))
          elif enqueued == 0:
            stdscr.addstr('0', curses.color_pair(2))
          if s.get('changediffhist-enqueueCount'):
            stdscr.move(reserved_rows + n, col + longestenq)
            arrow(s.get('changediffhist-enqueueCount', 0))
#          stdscr.move(reserved_rows + n, col + longestenq + 5)
#          stdscr.addstr(str(s.get('changediffhist-enqueueCount', '')))

        col += longestenq + 1
        longestsize = max(len(str(s.get('size', 0))) for s in status_list)
        for n, s in enumerate(status_list):
          stdscr.move(reserved_rows + n, col)
          if s.get('change-enqueueCount') > 0:
            stdscr.addstr(u'\u25b6'.encode('utf-8'), curses.color_pair(3))
          else:
            stdscr.addstr(u'\u25b6'.encode('utf-8'), curses.color_pair(2))
          if s.get('size') > 0:
            stdscr.addstr(u'\u2772'.encode('utf-8'), curses.color_pair(5) + curses.A_BOLD)
            stdscr.move(reserved_rows + n, col + 2 + longestsize - len(str(s['size'])))
            if s.get('change-enqueueCount') > 0 and s.get('change-dequeueCount') == 0:
              stdscr.addstr(str(s['size']), curses.color_pair(1) + curses.A_BOLD)
            else:
              stdscr.addstr(str(s['size']), curses.color_pair(5))
            arrow(s.get('changehist-size', 0))
          else:
            stdscr.addstr(u'\u2772'.encode('utf-8'), curses.color_pair(2) + curses.A_BOLD)

        col += longestsize + 3
        longestflight = max(len(str(s.get('inflightCount', 0))) for s in status_list)
        for n, s in enumerate(status_list):
          stdscr.move(reserved_rows + n, col)
          if s.get('size') > 0:
            stdscr.addstr('|', curses.color_pair(5) + curses.A_BOLD)
          else:
            stdscr.addstr('|', curses.color_pair(2) + curses.A_BOLD)
          if s.get('inflightCount', 0) > 0:
            stdscr.move(reserved_rows + n, col + 1 + longestflight - len(str(s['inflightCount'])))
            stdscr.addstr(str(s['inflightCount']), curses.color_pair(5))
            arrow(s.get('changehist-inflightCount'))
          stdscr.move(reserved_rows + n, col + longestflight + 2)
          if s.get('size') > 0:
            stdscr.addstr(u'\u2773'.encode('utf-8'), curses.color_pair(5) + curses.A_BOLD)
          else:
            stdscr.addstr(u'\u2773'.encode('utf-8'), curses.color_pair(2) + curses.A_BOLD)

        col += longestflight + 3
        for n, s in enumerate(status_list):
          stdscr.move(reserved_rows + n, col)
          dequeued = s.get('change-dequeueCount')
          if not dequeued:
            stdscr.addstr(u'\u25b6'.encode('utf-8'), curses.color_pair(2))
          elif dequeued > 0:
            stdscr.addstr(u'\u25b6'.encode('utf-8'), curses.color_pair(3))
            stdscr.addstr(str(dequeued), curses.color_pair(3))
            arrow(s.get('changediffhist-dequeueCount', 0))
          elif dequeued == 0:
            stdscr.addstr(u'\u25b60'.encode('utf-8'), curses.color_pair(2))

#         stdscr.addstr(reserved_rows + n, curses.COLS - 7, "%6.1f" % s['relevance'])
        stdscr.refresh()
        time.sleep(0.4)
    except KeyboardInterrupt:
      return

  def process_report(self, report, destination, dest):
    self.last_gather = time.time()
    shortdest = dest.replace('uk.ac.diamond.', 'u.a.d.').replace('transient.', 't.')
    shortdest = re.sub('([0-9a-f]{8})-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '\\1-(..)', shortdest)
    report['shortdest'] = shortdest
    report['last-seen'] = time.time()

    with self.lock:
      last_status = self.status.get(destination, {})
      report['size'] = report['QueueSize']
      report['enqueueCount'] = report['EnqueueCount']
      report['dequeueCount'] = report['DequeueCount']
      report['consumerCount'] = report['ConsumerCount']
      report['producerCount'] = report['ProducerCount']
      report['inflightCount'] = report['InFlightCount']
      report['destinationName'] = destination
      for key in ('size', 'enqueueCount', 'dequeueCount', 'consumerCount', 'producerCount', 'inflightCount'):
        if key in last_status:
          report['change-' + key] = report.get(key, 0) - last_status[key]

        # Find change trend (2nd derivative)
        report['changehist-' + key] = last_status.get('changehist-' + key, 0)
        if 'change-' + key in report:
          change = report['change-' + key]
          # This is the absolute change in the value since the last update
          if change < 0:
            report['changehist-' + key] = max(report['changehist-' + key] - 1, -3)
          elif change > 0:
            report['changehist-' + key] = min(report['changehist-' + key] + 1, 3)
          elif report['changehist-' + key] > 0.5:
            report['changehist-' + key] -= 0.5
          elif report['changehist-' + key] < -0.5:
            report['changehist-' + key] += 0.5
          else:
            report['changehist-' + key] = 0
        # Find change trend trend (3nd derivative)
        report['changediffhist-' + key] = last_status.get('changediffhist-' + key, 0)
        if 'change-' + key in last_status:
          change = report['change-' + key] - last_status['change-' + key]
          # This is the change in the (change in the value) since the last update
          if change < 0:
            report['changediffhist-' + key] = max(report['changediffhist-' + key] - 1, -3)
          elif change > 0:
            report['changediffhist-' + key] = min(report['changediffhist-' + key] + 1, 3)
          elif report['changediffhist-' + key] > 0.5:
            report['changediffhist-' + key] -= 0.5
          elif report['changediffhist-' + key] < -0.5:
            report['changediffhist-' + key] += 0.5
          else:
            report['changediffhist-' + key] = 0

      # Define a sort order by traffic levels (large numbers, large changes == important)
      relevance = last_status.get('relevance', 0) / 2
      for key in ('size', 'enqueueCount', 'dequeueCount'):
        for qualifier in ('', 'change-'):
          relevance = relevance + math.log(1 + abs(report.get(qualifier + key, 0)))
        for qualifier in ('changehist-', 'changediffhist-'):
          relevance = relevance + (abs(report.get(qualifier + key, 0)))
      report['relevance'] = relevance

      self.status[destination] = report

#averageEnqueueTime :  0
#averageMessageSize :  0
#brokerId :  [u'ID:cs04r-sc-vserv-128.diamond.ac.uk-45333-1480578321703-0:1']
#brokerName :  [u'localhost']
#consumerCount :  1
#dequeueCount :  1
#destinationName :  [u'queue://zocdev.transient.system_test.c01d6377-2e4c-4bbb-8e7e-c93bcd839568.6']
#dispatchCount :  0
#enqueueCount :  1
#expiredCount :  1
#inflightCount :  0
#maxEnqueueTime :  0
#memoryLimit :  668309914
#memoryPercentUsage :  0
#memoryUsage :  0
#messagesCached :  0
#minEnqueueTime :  0
#producerCount :  0
#size :  0

if __name__ == '__main__':
  parser = OptionParser(
    usage='dlstbx.queue_monitor [options]'
  )
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)

  parser.add_option("--test", action="store_true", dest="test", help="Run in ActiveMQ testing (zocdev) namespace")
  default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'
  if '--test' in sys.argv:
    default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg'

  (options, args) = parser.parse_args()

  QueueStatus().run()
