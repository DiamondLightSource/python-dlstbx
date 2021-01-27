#
# dlstbx.status_monitor
#   Starts a status monitor (what do you expect?)
#


import curses
import logging
import os
import re
import sys
import threading
import time
from optparse import SUPPRESS_HELP, OptionParser
from pprint import pprint

import workflows
import workflows.transport
from dlstbx.util.version import dlstbx_version
from workflows.services.common_service import CommonService

# Our conda-based installers are fundamentally broken.
# Thankfully this only manifests in curses.
# Oh, and the curses package is broken, too.
# Fix by telling the package where the relocated terminfo lives
os.environ["TERMINFO"] = os.path.abspath(
    os.path.join(__file__, "..", "..", "..", "..", "conda_base", "share", "terminfo")
)


class Monitor:
    """A sample implementation of a status monitor showing all running services"""

    shutdown = False
    """Set to true to end the main loop and shut down the service monitor."""

    cards = {}
    """Register card shown for seen services"""

    border_chars = ()
    """Characters used for frame borders."""
    border_chars_text = ("|", "|", "=", "=", "/", "\\", "\\", "/")
    """Example alternative set of frame border characters."""

    most_recent_version = {}
    """Dictionary to hold software version information, so old versions can be highlighted."""

    def __init__(self, filters=None, transport=None, version=None, test=False):
        """Set up monitor and connect to the network transport layer"""
        if transport is None or isinstance(transport, str):
            self._transport = workflows.transport.lookup(transport)()
        else:
            self._transport = transport()
        assert self._transport.connect(), "Could not connect to transport layer"
        self._filters = filters
        self._lock = threading.RLock()
        self._node_status = {}
        self.headline = "DLS " + ("ZocDEV" if test else "Zocalo") + " service monitor"
        if version:
            self.headline += " v%s" % version.split(" ")[1].split("-")[0]
        self.headline += " -- quit with Ctrl+C"
        self.log_box = None
        self._transport.subscribe_broadcast(
            "transient.status", self.update_status, retroactive=True
        )
        self.last_info = None
        self.last_info_messages = 0
        self._transport.subscribe_broadcast("transient.log", self.print_log_message)

        dlstbx_version_num = re.search("dlstbx ([0-9.]*)", dlstbx_version()).group(1)
        self.version_dlstbx = tuple(int(i) for i in dlstbx_version_num.split("."))
        self.version_workflows = tuple(int(i) for i in workflows.version().split("."))

    def print_log_message(self, header, message):
        """Add a new log message to the log window."""
        with self._lock:
            if self.log_box:
                if not isinstance(message, dict) or "message" not in message:
                    self.log_box.addstr(
                        "Unknown message:\n" + message, curses.color_pair(1)
                    )
                else:
                    if message["name"] == "dlstbx.services.cluster.stats":
                        return  # Filter cluster statistics messages
                    message["service_description"] = message.get(
                        "workflows_service", ""
                    )
                    if "workflows_statustext" in message:
                        message[
                            "service_description"
                        ] = " ({workflows_service}:{workflows_statustext})".format(
                            **message
                        )
                    message["workflows_host"] = message.get("workflows_host", "???")
                    if (
                        self.last_info
                        != [
                            message.get(x)
                            for x in (
                                "workflows_host",
                                "workflows_service",
                                "workflows_status",
                            )
                        ]
                        or self.last_info_messages > 20
                    ):
                        self.last_info = [
                            message.get(x)
                            for x in (
                                "workflows_host",
                                "workflows_service",
                                "workflows_status",
                            )
                        ]
                        self.last_info_messages = 0
                        self.log_box.addstr(
                            "====== {workflows_host}{service_description} ======\n".format(
                                **message
                            ),
                            curses.A_BOLD,
                        )
                    self.last_info_messages += 1
                    msg_col = curses.color_pair(5)
                    if message["levelno"] >= logging.INFO:
                        msg_col = curses.color_pair(3)
                    if message["levelno"] >= logging.WARN:
                        msg_col = curses.color_pair(4)
                    if message["levelno"] >= logging.ERROR:
                        msg_col = curses.color_pair(1)
                    if message["levelno"] >= logging.CRITICAL:
                        msg_col = curses.color_pair(1) + curses.A_BOLD
                    if message.get("exc_text"):
                        self.log_box.addstr(
                            "{name}: {msg}{service_description}\n".format(**message),
                            msg_col,
                        )
                        self.log_box.addstr(str(message["exc_text"]) + "\n", msg_col)
                    else:
                        if message["levelno"] >= logging.WARN:
                            self.log_box.addstr(
                                "{pathname}:{lineno}{service_description}\n".format(
                                    **message
                                ),
                                msg_col,
                            )
                        self.log_box.addstr(
                            "{name}: {msg}\n".format(**message), msg_col
                        )
                self.log_box.refresh()

    def _is_most_recent_version(self, program, version):
        try:
            vnum = tuple(int(i) for i in version.split("."))
        except ValueError:
            return False
        if program not in self.most_recent_version:
            self.most_recent_version[program] = (version, vnum)
            return True
        elif self.most_recent_version[program][0] == version:
            return True
        else:
            if vnum > self.most_recent_version[program][1]:
                self.most_recent_version[program] = (version, vnum)
                return True
        return False

    def _is_outdated_version(self, program, version):
        try:
            vnum = tuple(int(i) for i in version.split("."))
        except ValueError:
            return True
        if program == "dlstbx":
            return vnum < self.version_dlstbx
        elif program == "workflows":
            return vnum < self.version_workflows
        return False

    def update_status(self, header, message):
        """Process incoming status message. Acquire lock for status dictionary before updating."""
        if self._filters:
            if any(not f(message) for f in self._filters):
                return  # skip
        with self._lock:
            if (
                message["host"] not in self._node_status
                or int(header["timestamp"])
                >= self._node_status[message["host"]]["last_seen"]
            ):
                if "dlstbx" in message:
                    match = re.search("dlstbx ([0-9.]*)", message["dlstbx"])
                    if match:
                        message["dlstbx"] = match.group(1)
                    else:
                        message["dlstbx"] = None
                self._node_status[message["host"]] = message
                self._node_status[message["host"]]["last_seen"] = int(
                    header["timestamp"]
                )

    def run(self):
        """A wrapper for the real _run() function to cleanly enable/disable the
        curses environment."""
        curses.wrapper(self._run)

    #    import mock
    #    self._run(mock.Mock())

    def _boxwin(
        self, height, width, row, column, title=None, title_x=7, color_pair=None
    ):
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
        """Redraw screen. This could be to initialize, or to redraw after resizing."""
        with self._lock:
            stdscr.clear()
            stdscr.addstr(0, 0, self.headline, curses.A_BOLD)
            stdscr.refresh()
            self.cards = []
            self._redraw_log_box()

    def _redraw_log_box(self, reserved_card_spaces=0):
        with self._lock:
            starty = 2
            if self.cards or reserved_card_spaces:
                max_cards_horiz = int(curses.COLS / 35)
                starty = 2 + 6 * (
                    (len(self.cards) + reserved_card_spaces + max_cards_horiz - 1)
                    // max_cards_horiz
                )
            height = curses.LINES - starty
            if self.log_box:
                oldstarty = self.log_box.getbegyx()[0] - 1
                oldheight = self.log_box.getmaxyx()[0]
                if starty == oldstarty and curses.LINES == oldstarty + oldheight + 2:
                    return  # No change needed
                obliterate = curses.newwin(
                    curses.LINES - oldstarty, curses.COLS, oldstarty, 0
                )
                obliterate.erase()
                obliterate.noutrefresh()
            if self.log_box:
                self._boxwin(height, curses.COLS, starty, 0, title="log")
                self.log_box.resize(height - 2, curses.COLS - 2)
                self.log_box.mvwin(starty + 1, 1)
            else:
                self.log_box = self._boxwin(height, curses.COLS, starty, 0, title="log")
                self.log_box.scrollok(True)
            self.log_box.noutrefresh()

    def _get_card(self, number):
        with self._lock:
            if number < len(self.cards):
                return self.cards[number]
            if number == len(self.cards):
                max_cards_horiz = int(curses.COLS / 35)
                max_cards_vert = int((curses.LINES - 2 - 7) / 6)
                if (number // max_cards_horiz) >= max_cards_vert:
                    return  # Don't add more cards - screen is full
                self._redraw_log_box(reserved_card_spaces=1)
                self.cards.append(
                    self._boxwin(
                        6,
                        35,
                        2 + 6 * (number // max_cards_horiz),
                        35 * (number % max_cards_horiz),
                        color_pair=3,
                    )
                )
                return self.cards[number]
            return

    def _erase_card(self, number):
        """Destroy cards with this or higher number."""
        with self._lock:
            if number < (len(self.cards) - 1):
                self._erase_card(number + 1)
            if number > (len(self.cards) - 1):
                return
            max_cards_horiz = int(curses.COLS / 35)
            obliterate = curses.newwin(
                6,
                35,
                2 + 6 * (number // max_cards_horiz),
                35 * (number % max_cards_horiz),
            )
            obliterate.erase()
            obliterate.noutrefresh()
            del self.cards[number]
            self._redraw_log_box()

    def _run(self, stdscr):
        """Start the actual service monitor"""
        screeny, screenx = stdscr.getmaxyx()
        with self._lock:
            curses.use_default_colors()
            curses.curs_set(False)
            curses.init_pair(1, curses.COLOR_RED, -1)
            curses.init_pair(2, curses.COLOR_BLACK, -1)
            curses.init_pair(3, curses.COLOR_GREEN, -1)
            curses.init_pair(4, curses.COLOR_YELLOW, -1)
            curses.init_pair(5, curses.COLOR_BLUE, -1)
            self._redraw_screen(stdscr)

        try:
            while not self.shutdown and self._transport.is_connected():
                # Check if screen was re-sized (True or False)
                resize = curses.is_term_resized(screeny, screenx)
                if resize:
                    screeny, screenx = stdscr.getmaxyx()
                    stdscr.clear()
                    curses.resizeterm(screeny, screenx)
                    stdscr.refresh()
                    self._redraw_screen(stdscr)

                now = int(time.time())
                with self._lock:
                    overview = self._node_status.copy()

                overview_keys = sorted(
                    (
                        overview[key].get("service") or "",
                        overview[key].get("dlstbx") or "",
                        overview[key].get("workflows") or "",
                        overview[key]["host"],
                        key,
                    )
                    for key in list(overview)
                )
                overview_keys = [x[-1] for x in overview_keys]
                cardnumber = 0
                for host in overview_keys:
                    status = overview[host]
                    age = now - int(status["last_seen"] / 1000)
                    with self._lock:
                        if (
                            age > 90
                            or (
                                age > 10
                                and status["status"]
                                == CommonService.SERVICE_STATUS_TEARDOWN
                            )
                            or (
                                age > 10
                                and status["status"] == CommonService.SERVICE_STATUS_END
                                and "task" in status
                            )
                        ):
                            del self._node_status[host]
                        else:
                            card = self._get_card(cardnumber)
                            if card:
                                card.erase()
                                card.move(0, 0)
                                if "service" in status and status["service"]:
                                    card.addstr("Service: ", curses.color_pair(3))
                                    card.addstr(status["service"])
                                elif "task" in status and status["task"]:
                                    card.addstr("Task: ", curses.color_pair(3))
                                    card.addstr(status["task"])
                                else:
                                    card.addstr("Service: ", curses.color_pair(3))
                                    card.addstr("---", curses.color_pair(2))
                                card.move(1, 0)
                                card.addstr("Host: ", curses.color_pair(3))
                                if host.startswith("uk.ac.diamond."):
                                    host = host[14:]
                                host = host.split(".")
                                if len(host) >= 2:
                                    card.addstr(".".join(host[:-1]))
                                    card.addstr(
                                        "." + host[-1],
                                        curses.color_pair(2) + curses.A_BOLD,
                                    )
                                else:
                                    card.addstr(host)
                                card.move(2, 0)
                                card.addstr("State: ", curses.color_pair(3))
                                if "status" in status:
                                    status_code = status["status"]
                                    state_string = (
                                        CommonService.human_readable_state.get(
                                            status_code, str(status_code)
                                        )
                                    )
                                    state_color = None
                                    if status_code in (
                                        CommonService.SERVICE_STATUS_PROCESSING,
                                        CommonService.SERVICE_STATUS_TIMER,
                                    ):
                                        state_color = (
                                            curses.color_pair(3) + curses.A_BOLD
                                        )
                                    if status_code == CommonService.SERVICE_STATUS_IDLE:
                                        state_color = (
                                            curses.color_pair(2) + curses.A_BOLD
                                        )
                                    if status_code in (
                                        CommonService.SERVICE_STATUS_ERROR,
                                        CommonService.SERVICE_STATUS_TEARDOWN,
                                    ):
                                        state_color = (
                                            curses.color_pair(1) + curses.A_BOLD
                                        )
                                    if state_color:
                                        card.addstr(state_string, state_color)
                                    else:
                                        card.addstr(state_string)
                                card.move(3, 0)
                                if age >= 10:
                                    card.addstr(
                                        "last seen %d seconds ago" % age,
                                        curses.color_pair(1)
                                        + (0 if age < 60 else curses.A_BOLD),
                                    )
                                else:
                                    card.addstr("V: ", curses.color_pair(3))
                                    if "dlstbx" in status:
                                        card.addstr("dlstbx ")
                                        if self._is_outdated_version(
                                            "dlstbx", status["dlstbx"]
                                        ):
                                            card.addstr(
                                                status["dlstbx"], curses.color_pair(1)
                                            )
                                        elif not self._is_most_recent_version(
                                            "dlstbx", status["dlstbx"]
                                        ):
                                            card.addstr(
                                                status["dlstbx"], curses.color_pair(4)
                                            )
                                        else:
                                            card.addstr(status["dlstbx"])
                                        card.addstr(", ")
                                    card.addstr("WF ")
                                    if self._is_outdated_version(
                                        "workflows", status["workflows"]
                                    ):
                                        card.addstr(
                                            status["workflows"], curses.color_pair(1)
                                        )
                                    elif not self._is_most_recent_version(
                                        "workflows", status["workflows"]
                                    ):
                                        card.addstr(
                                            status["workflows"], curses.color_pair(4)
                                        )
                                    else:
                                        card.addstr(status["workflows"])
                                if "utilization" in status:
                                    card.move(3, 28)
                                    busy = sum(
                                        status["utilization"].get(s, 0)
                                        for s in ("1", "3", "4", "5")
                                    )
                                    busy = round(busy * 100)

                                    state_color = None
                                    if busy < 10:
                                        state_color = curses.color_pair(2)
                                    elif busy > 90:
                                        state_color = (
                                            curses.color_pair(3) + curses.A_BOLD
                                        )
                                    elif busy > 75:
                                        state_color = curses.color_pair(3)
                                    if state_color:
                                        card.addstr("%3d%%" % busy, state_color)
                                    else:
                                        card.addstr("%3d%%" % busy)
                                card.noutrefresh()
                            cardnumber = cardnumber + 1
                if cardnumber < len(self.cards):
                    with self._lock:
                        self._erase_card(cardnumber)
                with self._lock:
                    curses.doupdate()
                if not resize:
                    time.sleep(0.2)
        except KeyboardInterrupt:
            """User pressed CTRL+C"""
            pass
        self._transport.disconnect()


class RawMonitor:
    """A minimalistic monitor that only displays raw status messages."""

    def __init__(self, filters=None, transport=None, version=None, test=False):
        """Set up monitor and connect to the network transport layer"""
        if transport is None or isinstance(transport, str):
            self._transport = workflows.transport.lookup(transport)()
        else:
            self._transport = transport()
        assert self._transport.connect(), "Could not connect to transport layer"
        self._lock = threading.RLock()
        self._filters = filters
        headline = "DLS " + ("ZocDEV" if test else "Zocalo") + " service monitor"
        if version:
            headline += " v%s" % version.split(" ")[1].split("-")[0]
        headline += " -- quit with Ctrl+C"
        print(headline)

    def print_status(self, header, message):
        if self._filters:
            if any(not f(message) for f in self._filters):
                return  # skip
        pprint(message)

    def run(self):
        self._transport.subscribe_broadcast(
            "transient.status", self.print_status, retroactive=True
        )
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    version = dlstbx_version()
    parser = OptionParser(usage="dlstbx.status_monitor [options]", version=version)
    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option(
        "-n",
        action="store_true",
        dest="nofancy",
        default=False,
        help="Do not draw fancy borders",
    )
    parser.add_option(
        "-t",
        "--transport",
        dest="transport",
        metavar="TRN",
        default="stomp",
        help="Transport mechanism, default '%default'",
    )
    parser.add_option(
        "--host",
        dest="hostfilter",
        default=None,
        help="Filter to hosts matching this regular expression",
    )

    parser.add_option(
        "--test",
        action="store_true",
        dest="test",
        help="Run in ActiveMQ testing (zocdev) namespace",
    )
    parser.add_option(
        "--raw", action="store_true", dest="raw", help="Show raw status messages"
    )
    default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-live.cfg"
    if "--test" in sys.argv:
        default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-testing.cfg"

    # override default stomp host
    from workflows.transport.stomp_transport import StompTransport

    StompTransport.load_configuration_file(default_configuration)

    workflows.transport.add_command_line_options(parser)
    (options, args) = parser.parse_args()

    monitor = Monitor
    if options.raw:
        monitor = RawMonitor

    filters = []
    if options.hostfilter:
        matcher = re.compile(options.hostfilter)

        def is_host_match(message):
            return bool(matcher.search(message["host"]))

        filters.append(is_host_match)

    monitor = monitor(
        transport=options.transport, version=version, filters=filters, test=options.test
    )
    if options.nofancy:
        monitor.border_chars = monitor.border_chars_text
    monitor.run()
