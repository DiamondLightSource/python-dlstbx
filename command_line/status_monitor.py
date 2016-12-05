#
# dlstbx.status_monitor
#   Starts a status monitor (what do you expect?)
#

from __future__ import absolute_import, division
import curses
from dlstbx.util.version import dlstbx_version
import logging
from optparse import OptionParser, SUPPRESS_HELP
import threading
import time
from workflows.services.common_service import CommonService
import workflows.transport

class Monitor():
  '''A sample implementation of a status monitor showing all running services'''

  shutdown = False
  '''Set to true to end the main loop and shut down the service monitor.'''

  cards = {}
  '''Register card shown for seen services'''

  border_chars = ()
  '''Characters used for frame borders.'''
  border_chars_text = ('|', '|', '=', '=', '/', '\\', '\\', '/')
  '''Example alternative set of frame border characters.'''

  def __init__(self, transport=None):
    '''Set up monitor and connect to the network transport layer'''
    if transport is None or isinstance(transport, basestring):
      self._transport = workflows.transport.lookup(transport)()
    else:
      self._transport = transport()
    assert self._transport.connect(), "Could not connect to transport layer"
    self._lock = threading.RLock()
    self._node_status = {}
    self.message_box = None
    self.log_box = None
    self._transport.subscribe_broadcast('transient.status', self.update_status, retroactive=True)
    self.last_info = None
    self.last_info_messages = 0
    self._transport.subscribe_broadcast('transient.log', self.print_log_message)

  def print_log_message(self, header, message):
    '''Add a new log message to the log window.'''
    with self._lock:
      if self.log_box:
        if not isinstance(message, dict) or 'message' not in message:
          self.log_box.addstr("Unknown message:\n" + message, curses.color_pair(1))
        else:
          message['service_description'] = message.get('workflows_service', '')
          if 'workflows_statustext' in message:
            message['service_description'] = ' ({workflows_service}:{workflows_statustext})'.format(**message)
          message['workflows_host'] = message.get('workflows_host', '???')
          if self.last_info != [message.get(x) for x in ('workflows_host', 'workflows_service', 'workflows_status')] or self.last_info_messages > 20:
            self.last_info = [message.get(x) for x in ('workflows_host', 'workflows_service', 'workflows_status')]
            self.last_info_messages = 0
            self.log_box.addstr("====== {workflows_host}{service_description} ======\n".format(**message), curses.A_BOLD)
          self.last_info_messages += 1
          msg_col = curses.color_pair(3)
          if message['levelno'] >= logging.INFO:
            msg_col = curses.color_pair(3)
          if message['levelno'] >= logging.WARN:
            msg_col = curses.color_pair(4)
          if message['levelno'] >= logging.ERROR:
            msg_col = curses.color_pair(1)
          if message['levelno'] >= logging.CRITICAL:
            msg_col = curses.color_pair(1) + curses.A_BOLD
          if message.get('exc_text'):
            self.log_box.addstr("{name}: {msg}{service_description}\n".format(**message), msg_col)
            self.log_box.addstr(str(message['exc_text']) + "\n", msg_col)
          else:
            if message['levelno'] >= logging.WARN:
              self.log_box.addstr("{pathname}:{lineno}{service_description}\n".format(**message), msg_col)
            self.log_box.addstr("{name}: {msg}\n".format(**message), msg_col)
        self.log_box.refresh()

  def update_status(self, header, message):
    '''Process incoming status message. Acquire lock for status dictionary before updating.'''
    with self._lock:
      if self.message_box:
        self.message_box.erase()
        self.message_box.move(0,0)
        for n, field in enumerate(header):
          if n == 0:
            self.message_box.addstr(field + ":", curses.color_pair(1))
          else:
            self.message_box.addstr(", " + field + ":", curses.color_pair(1))
          self.message_box.addstr(header[field])
        self.message_box.addstr(": ", curses.color_pair(1))
        self.message_box.addstr(str(message), curses.color_pair(2) + curses.A_BOLD)
        self.message_box.refresh()
      if message['host'] not in self._node_status or \
          int(header['timestamp']) >= self._node_status[message['host']]['last_seen']:
        self._node_status[message['host']] = message
        self._node_status[message['host']]['last_seen'] = int(header['timestamp'])

  def run(self):
    '''A wrapper for the real _run() function to cleanly enable/disable the
       curses environment.'''
    curses.wrapper(self._run)

  def _boxwin(self, height, width, row, column, title=None, title_x=7, color_pair=None):
    with self._lock:
      box = curses.newwin(height, width, row, column)
      box.clear()
      if color_pair:
        box.attron(curses.color_pair(color_pair))
      box.border(*self.border_chars)
      if title:
        box.addstr(0, title_x, " " + title + " ")
      if color_pair:
        box.attroff(curses.color_pair(color_pair))
      box.noutrefresh()
      return curses.newwin(height - 2, width - 2, row + 1, column + 1)

  def _redraw_screen(self, stdscr):
    '''Redraw screen. This could be to initialize, or to redraw after resizing.'''
    with self._lock:
      stdscr.clear()
      stdscr.addstr(0, 0, "DLS Zocalo service monitor -- quit with Ctrl+C", curses.A_BOLD)
      stdscr.refresh()
      self.message_box = self._boxwin(5, curses.COLS, 2, 0, title='last seen message', color_pair=1)
      self.message_box.scrollok(True)
      self.cards = []
      self._redraw_log_box()

  def _redraw_log_box(self, reserved_card_spaces=0):
    with self._lock:
      starty = 7
      if self.cards or reserved_card_spaces:
        max_cards_horiz = int(curses.COLS / 35)
        starty = 7 + 6 * ((len(self.cards) + reserved_card_spaces + max_cards_horiz - 1) // max_cards_horiz)
      height = curses.LINES - starty
      if self.log_box:
        oldstarty = self.log_box.getbegyx()[0] - 1
        oldheight = self.log_box.getmaxyx()[0]
        if starty == oldstarty and curses.LINES == oldstarty + oldheight + 2:
          return # No change needed
        obliterate = curses.newwin(curses.LINES - oldstarty, curses.COLS, oldstarty, 0)
        obliterate.erase()
        obliterate.noutrefresh()
      if self.log_box:
        self._boxwin(height, curses.COLS, starty, 0, title='log')
        self.log_box.resize(height - 2, curses.COLS - 2)
        self.log_box.mvwin(starty + 1, 1)
      else:
        self.log_box = self._boxwin(height, curses.COLS, starty, 0, title='log')
        self.log_box.scrollok(True)
      self.log_box.noutrefresh()

  def _get_card(self, number):
    with self._lock:
      if number < len(self.cards):
        return self.cards[number]
      if number == len(self.cards):
        max_cards_horiz = int(curses.COLS / 35)
        max_cards_vert = int((curses.LINES - 7 - 7)/ 6)
        if (number // max_cards_horiz) >= max_cards_vert:
          return # Don't add more cards - screen is full
        self._redraw_log_box(reserved_card_spaces=1)
        self.cards.append(self._boxwin(6, 35, 7 + 6 * (number // max_cards_horiz), 35 * (number % max_cards_horiz), color_pair=3))
        return self.cards[number]
      return

  def _erase_card(self, number):
    '''Destroy cards with this or higher number.'''
    with self._lock:
      if number < (len(self.cards) - 1):
        self._erase_card(number + 1)
      if number > (len(self.cards) - 1):
        return
      max_cards_horiz = int(curses.COLS / 35)
      obliterate = curses.newwin(6, 35, 7 + 6 * (number // max_cards_horiz), 35 * (number % max_cards_horiz))
      obliterate.erase()
      obliterate.noutrefresh()
      del(self.cards[number])
      self._redraw_log_box()

  def _run(self, stdscr):
    '''Start the actual service monitor'''
    with self._lock:
      curses.use_default_colors()
      curses.curs_set(False)
      curses.init_pair(1, curses.COLOR_RED, -1)
      curses.init_pair(2, curses.COLOR_BLACK, -1)
      curses.init_pair(3, curses.COLOR_GREEN, -1)
      curses.init_pair(4, curses.COLOR_YELLOW, -1)
      self._redraw_screen(stdscr)

    try:
      while not self.shutdown and self._transport.is_connected():
        now = int(time.time())
        with self._lock:
          overview = self._node_status.copy()
        cardnumber = 0
        for host, status in overview.iteritems():
          age = (now - int(status['last_seen'] / 1000))
          with self._lock:
            if age > 90:
              del(self._node_status[host])
            else:
              card = self._get_card(cardnumber)
              if card:
                card.erase()
                card.move(0, 0)
                card.addstr('Host: ', curses.color_pair(3))
                if host.startswith('uk.ac.diamond.'): host = host[14:]
                host = host.split('.')
                if len(host) >= 2:
                  card.addstr('.'.join(host[:-1]))
                  card.addstr('.' + host[-1],
                    curses.color_pair(2) + curses.A_BOLD)
                else:
                  card.addstr(host)
                card.move(1, 0)
                card.addstr('Service: ', curses.color_pair(3))
                if 'service' in status and status['service']:
                  card.addstr(status['service'])
                else:
                  card.addstr('---', curses.color_pair(2))
                card.move(2, 0)
                card.addstr('State: ', curses.color_pair(3))
                if 'status' in status:
                  status_code = status['status']
                  state_string = CommonService.human_readable_state.get(status_code, str(status_code))
                  state_color = None
                  if status_code in (CommonService.SERVICE_STATUS_PROCESSING, CommonService.SERVICE_STATUS_TIMER):
                    state_color = curses.color_pair(3) + curses.A_BOLD
                  if status_code == CommonService.SERVICE_STATUS_IDLE:
                    state_color = curses.color_pair(2) + curses.A_BOLD
                  if status_code == CommonService.SERVICE_STATUS_ERROR:
                    state_color = curses.color_pair(1)
                  if state_color:
                    card.addstr(state_string, state_color)
                  else:
                    card.addstr(state_string)
                card.move(3, 0)
                if age >= 10:
                  card.addstr("last seen %d seconds ago" % age, curses.color_pair(1) + (0 if age < 60 else curses.A_BOLD))
                card.noutrefresh()
              cardnumber = cardnumber + 1
        if cardnumber < len(self.cards):
          with self._lock:
            self._erase_card(cardnumber)
        with self._lock:
          curses.doupdate()
        time.sleep(0.2)
    except KeyboardInterrupt:
      '''User pressed CTRL+C'''
      pass
    self._transport.disconnect()

if __name__ == '__main__':
  # override default stomp host
  from workflows.transport.stomp_transport import StompTransport
  try:
    StompTransport.load_configuration_file(
        '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg')
  except workflows.WorkflowsError, e:
    print e # probably should use logging

  parser = OptionParser(
    usage='dlstbx.status_monitor [options]',
    version=dlstbx_version()
  )
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-n", action="store_true", dest="nofancy",
      default=False, help="Do not draw fancy borders")
  parser.add_option("-t", "--transport", dest="transport", metavar="TRN",
      default="stomp", help="Transport mechanism, default '%default'")
  workflows.transport.add_command_line_options(parser)
  (options, args) = parser.parse_args()

  monitor = Monitor(
      transport=options.transport,
    )
  if options.nofancy:
    monitor.border_chars = monitor.border_chars_text
  monitor.run()

