#
# dlstbx.graylog
#   'tail' equivalent for graylog messages
#

from __future__ import absolute_import, division, print_function

import datetime
import httplib
import socket
import string
import sys
import time
import urllib2
from optparse import SUPPRESS_HELP, OptionParser

from dlstbx.util.colorstreamhandler import ColorStreamHandler
from dlstbx.util.graylog import GraylogAPI

log_levels = {
    0: {"name": "emerg", "color": ColorStreamHandler.CRITICAL},
    1: {"name": "alert", "color": ColorStreamHandler.CRITICAL},
    2: {"name": "crit", "color": ColorStreamHandler.CRITICAL},
    3: {"name": "err", "color": ColorStreamHandler.ERROR},
    4: {"name": "warning", "color": ColorStreamHandler.WARNING},
    5: {"name": "notice", "color": ColorStreamHandler.INFO},
    6: {"name": "info", "color": ColorStreamHandler.INFO},
    7: {"name": "debug", "color": ColorStreamHandler.DEBUG},
}


def format_message(verbosity):
    class SafeDict(dict):
        """A dictionary that returns empty strings for undefined keys."""

        def __missing__(self, key):
            """Return an empty string."""
            assert key != "level"
            return {
                "BOLD": ColorStreamHandler.BOLD,
                "DEFAULT": ColorStreamHandler.DEFAULT,
                "LEVEL": log_levels[self["level"]]["color"],
            }.get(key, "")

    def format_string(fstring, message):
        return string.Formatter().vformat(fstring, (), SafeDict(message))

    def format_default(message):
        return (
            format_string(
                "{localtime:%Y-%m-%d %H:%M:%S}.{timestamp_msec}{LEVEL} {facility}\n"
                "                        {message}\n",
                message,
            )
            + ColorStreamHandler.DEFAULT
        )

    def format_source(message):
        return (
            format_string(
                "{localtime:%Y-%m-%d %H:%M:%S}.{timestamp_msec}{LEVEL} {DEFAULT}{BOLD}{facility}{DEFAULT}{LEVEL} {source} {DEFAULT}{workflows_service}{LEVEL}\n"
                "                        {message}\n",
                message,
            )
            + ColorStreamHandler.DEFAULT
        )

    def format_verbose(message):
        if not message.get("full_message") or message["full_message"] == message.get(
            "message"
        ):
            message["full_message"] = ""
        else:
            message["full_message"] = message["full_message"] + "\n"
        return (
            format_string(
                "{localtime:%Y-%m-%d %H:%M:%S}.{timestamp_msec} {BOLD}{facility}{DEFAULT}{LEVEL} {source} {DEFAULT}{workflows_service}\n"
                "{file}{SourceClassName}:{line}{SourceLineNumber} ({function}{SourceMethodName})\n"
                "{LEVEL}{message}\n" + "{full_message}\n",
                message,
            )
            + ColorStreamHandler.DEFAULT
        )

    def format_xverbose(message):
        if not message.get("full_message") or message["full_message"] == message.get(
            "message"
        ):
            message["full_message"] = ""
        else:
            message["full_message"] = message["full_message"] + "\n"
        if message.get("workflows_statustext"):
            message["workflows_statustext"] = (
                "Status:" + message["workflows_statustext"]
            )
        if message.get("workflows_workflows"):
            message["workflows_workflows"] = (
                "workflows " + message["workflows_workflows"]
            )
        return (
            format_string(
                "{localtime} {BOLD}{facility}{DEFAULT}{LEVEL} {source} {DEFAULT}{workflows_service}\n"
                "{process_name}:{thread_name}{Thread}  {workflows_statustext}  {workflows_workflows}  {workflows_dlstbx}  {recipe_ID}\n"
                "{file}{SourceClassName}:{line}{SourceLineNumber} ({function}{SourceMethodName})\n"
                "{LEVEL}{message}\n" + "{full_message}\n",
                message,
            )
            + ColorStreamHandler.DEFAULT
        )

    def format_raw(message):
        return (
            str(message["localtime"])
            + "\n"
            + log_levels[message["level"]]["color"]
            + "\n".join(
                "%23s: %s"
                % (key, str(message[key]).replace("\n", "\n" + " " * 23 + "| "))
                for key in sorted(message)
                if not key.startswith("gl2_")
                and key not in ("localtime", "stream", "streams", "timestamp_msec")
            )
            + "\n\n"
            + ColorStreamHandler.DEFAULT
        )

    formats = [
        format_default,
        format_source,
        format_verbose,
        format_xverbose,
        format_raw,
    ]
    try:
        return formats[verbosity]
    except IndexError:
        return formats[-1]


if __name__ == "__main__":
    parser = OptionParser(usage="dlstbx.graylog [options]")
    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option(
        "-f",
        "--follow",
        dest="follow",
        default=False,
        action="store_true",
        help="Keep showing log messages as they come in.",
    )
    parser.add_option(
        "-a",
        "--aggregate",
        dest="aggregate",
        default=False,
        action="store_true",
        help="Only show a single instance for similar messages",
    )
    parser.add_option(
        "-r",
        "--recipe",
        dest="recipe",
        default=None,
        action="store",
        help="Only show log messages relating to this recipe ID",
    )
    parser.add_option(
        "-s",
        "--source",
        "--facility",
        dest="facility",
        default=[],
        action="append",
        help="Only show log messages from this facility. When specified"
        "multiple times will show messages from any one of them.",
    )
    parser.add_option(
        "--level",
        dest="level",
        default="info",
        help="Show messages with this loglevel and higher. Valid options: alert, critical, error, warning, notice, info, debug",
    )
    parser.add_option(
        "--time",
        dest="time",
        default="600",
        help="Start showing messages from this far back in time. Seconds if no unit (s/m/h/d/w) specified.",
    )
    parser.add_option(
        "-v",
        "--verbose",
        dest="verbose",
        default=0,
        action="count",
        help="Show more detail (can be specified multiple times)",
    )
    (options, args) = parser.parse_args(sys.argv[1:])

    try:
        level = ["a", "c", "e", "w", "n", "i", "d"].index(options.level.lower()[0]) + 1
    except ValueError:
        sys.exit("Invalid loglevel specified.")

    if options.aggregate and options.follow:
        sys.exit("Options --follow and --aggregate are mutually exclusive.")

    if options.time.isdigit():
        options.time = int(options.time)
    elif options.time[:-1].isdigit():
        calculated_time = int(options.time[:-1]) * {
            "s": 1,
            "m": 60,
            "h": 3600,
            "d": 86400,
            "w": 604800,
        }.get(options.time[-1].lower(), 0)
        if not calculated_time:
            sys.exit("error: option --time: invalid value '%s'" % options.time)
        options.time = calculated_time
    else:
        sys.exit("error: option --time: invalid value '%s'" % options.time)

    g = GraylogAPI("/dls_sw/apps/zocalo/secrets/credentials-log.cfg")
    g.level = level
    if options.facility:
        g.filters.append("facility:(" + " OR ".join(options.facility) + ")")
    if options.recipe:
        g.filters.append("recipe_ID:" + options.recipe)
    format = format_message(options.verbose)
    try:
        if options.follow:
            while True:
                try:
                    for message in g.get_messages(time=options.time):
                        sys.stdout.write(format(message))
                except (socket.error, urllib2.URLError, httplib.BadStatusLine) as e:
                    sys.stdout.write(
                        "{DEFAULT}{localtime:%Y-%m-%d %H:%M:%S} Graylog update failed: {exception}\n".format(
                            DEFAULT=ColorStreamHandler.DEFAULT,
                            localtime=datetime.datetime.now(),
                            exception=str(e),
                        )
                    )
                sys.stdout.flush()
                time.sleep(0.7)
        elif options.aggregate:
            aggregate = {}
            for order, message in enumerate(g.get_all_messages(time=options.time)):
                try:
                    message_id = "{message[level]}:{message[file]}:{message[line]}".format(
                        message=message
                    )
                except KeyError:
                    message_id = message.get("_id")
                agg = aggregate.setdefault(message_id, {"count": 0})
                agg["count"] += 1
                agg["message"] = message
                agg["order"] = order
            aggregate = sorted(aggregate.values(), key=lambda a: a["order"])
            for message in aggregate:
                sys.stdout.write(format(message["message"]))
                if message["count"] > 1:
                    sys.stdout.write(
                        "(representative of a group of {c.BOLD}{m[count]}{c.DEFAULT} similar messages)\n".format(
                            m=message, c=ColorStreamHandler
                        )
                    )
                print()
        else:
            for message in g.get_all_messages(time=options.time):
                sys.stdout.write(format(message))
    except KeyboardInterrupt:
        print()
