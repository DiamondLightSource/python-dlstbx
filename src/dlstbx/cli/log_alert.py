#
# dlstbx.log_alert
#   record named pass/fail events in a log, These can be picked up by
#   a notification daemon
#


import dlstbx
import logging
import sys
from optparse import SUPPRESS_HELP, OptionParser


def run():
    parser = OptionParser(
        usage="dlstbx.log_alert -n NAME (--pass | --fail) [free text]"
    )
    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option(
        "-n",
        "--name",
        dest="name",
        default=None,
        action="store",
        help="Name of alert to be logged.",
    )
    parser.add_option(
        "--pass",
        dest="log_pass",
        default=False,
        action="store_true",
        help="Log a success event.",
    )
    parser.add_option(
        "--fail",
        dest="log_fail",
        default=False,
        action="store_true",
        help="Log a failure event.",
    )
    (options, args) = parser.parse_args(sys.argv[1:])

    if not options.name:
        parser.error("No alert name given")
    if not options.log_pass ^ options.log_fail:
        parser.error("You must specify either --pass or --fail")

    dlstbx.enable_graylog()
    alertlog = logging.getLogger("zocalo.alert")
    alertlog.setLevel(logging.DEBUG)

    if options.log_pass:
        logfunction = alertlog.info
        event = "PASS"
    else:
        logfunction = alertlog.warn
        event = "FAIL"

    free_text = " ".join(args)

    logfunction(
        f"{options.name}: {event} {free_text}",
        extra={
            "alert_name": options.name,
            "alert_event": event,
            "alert_text": free_text,
        },
    )
