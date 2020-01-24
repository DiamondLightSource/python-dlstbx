#
# dlstbx.dlq_reinject
#   Take a dead letter queue message from a file and send it back to its queue
#   for a retry.
#

from __future__ import absolute_import, division, print_function

import json
import os
import re
import select
import sys
import time
from optparse import SUPPRESS_HELP, OptionParser
from pprint import pprint

from workflows.transport.stomp_transport import StompTransport

if __name__ == "__main__":
    parser = OptionParser(usage="dlstbx.dlq_reinject [options] file [file [..]]")

    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option(
        "-r",
        "--remove",
        action="store_true",
        default=False,
        dest="remove",
        help="Delete file on successful reinjection",
    )
    parser.add_option(
        "--test",
        action="store_true",
        dest="test",
        help="Run in ActiveMQ testing (zocdev) namespace",
    )
    parser.add_option(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        dest="verbose",
        help="Show message contents",
    )
    parser.add_option(
        "-d",
        "--destination",
        action="store",
        default=None,
        dest="destination_override",
        help="Reinject messages to a different destination. Any name given must include the stomp prefix.",
    )
    parser.add_option(
        "-w",
        "--wait",
        default=None,
        dest="wait",
        help="Wait this many seconds between reinjections",
    )
    default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-live.cfg"
    redirect_live_to_testing = "--test" in sys.argv
    if redirect_live_to_testing:
        default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-testing.cfg"
    # override default stomp host
    StompTransport.load_configuration_file(default_configuration)

    StompTransport.add_command_line_options(parser)
    (options, args) = parser.parse_args()

    stdin = []
    if select.select([sys.stdin], [], [], 0.0)[0]:
        dlq_purge_filename_format = re.compile(r"^  \/")
        while True:
            line = sys.stdin.readline()
            if not line:
                break
            if dlq_purge_filename_format.match(line):
                stdin.append(line.strip())
        print("%d filenames read from stdin" % len(stdin))

    if not args and not stdin:
        print("No DLQ message files given.")
        sys.exit(0)

    stomp = StompTransport()
    stomp.connect()
    dlqprefix = stomp.get_namespace()

    first = True
    for dlqfile in args + stdin:
        if not os.path.exists(dlqfile):
            print("Ignoring missing file {}".format(dlqfile))
            continue
        if not first and options.wait:
            time.sleep(float(options.wait))
        first = False
        with open(dlqfile, "r") as fh:
            dlqmsg = json.load(fh)
        print("Parsing message from {}".format(dlqfile))
        if (
            not isinstance(dlqmsg, dict)
            or not dlqmsg.get("header")
            or not dlqmsg.get("message")
        ):
            sys.exit("File is not a valid DLQ message.")
        if options.verbose:
            pprint(dlqmsg)

        destination = (
            dlqmsg["header"]
            .get("original-destination", dlqmsg["header"]["destination"])
            .split("/", 2)
        )
        if destination[1] == "queue":
            print("sending...")
            send_function = stomp.send
        elif destination[1] == "topic":
            print("broadcasting...")
            send_function = stomp.broadcast
        else:
            sys.exit("Cannot process message, unknown message mechanism")
        if redirect_live_to_testing and destination[2].startswith("zocalo."):
            destination[2] = destination[2].replace("zocalo.", "zocdev.", 1)
        if options.destination_override:
            destination[2] = options.destination_override
        header = dlqmsg["header"]
        for drop_field in (
            "content-length",
            "destination",
            "expires",
            "message-id",
            "original-destination",
            "originalExpiration",
            "subscription",
            "timestamp",
            "redelivered",
        ):
            if drop_field in header:
                del header[drop_field]
        send_function(
            destination[2], dlqmsg["message"], headers=header, ignore_namespace=True
        )
        if options.remove:
            os.remove(dlqfile)
        print("Done.\n")

    stomp.disconnect()
