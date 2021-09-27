#
# dlstbx.dlq_reinject
#   Take a dead letter queue message from a file and send it back to its queue
#   for a retry.
#


import json
import os
import re
import select
import sys
import time
from optparse import SUPPRESS_HELP, OptionParser
from pprint import pprint

import workflows.transport
import zocalo.configuration


def run() -> None:
    zc = zocalo.configuration.from_file()
    zc.activate()
    default_transport = workflows.transport.default_transport
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
    parser.add_option(
        "-t",
        "--transport",
        dest="transport",
        metavar="TRN",
        default=default_transport,
        help="Transport mechanism. Known mechanisms: "
        + ", ".join(workflows.transport.get_known_transports())
        + " (default: %default)",
    )

    zc.add_command_line_options(parser)
    workflows.transport.add_command_line_options(parser)
    (options, args) = parser.parse_args()
    transport = workflows.transport.lookup(options.transport)()

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

    transport.connect()

    first = True
    for dlqfile in args + stdin:
        if not os.path.exists(dlqfile):
            print(f"Ignoring missing file {dlqfile}")
            continue
        if not first and options.wait:
            time.sleep(float(options.wait))
        first = False
        with open(dlqfile) as fh:
            dlqmsg = json.load(fh)
        print(f"Parsing message from {dlqfile}")
        if (
            not isinstance(dlqmsg, dict)
            or not dlqmsg.get("header")
            or not dlqmsg.get("message")
        ):
            sys.exit("File is not a valid DLQ message.")
        if options.verbose:
            pprint(dlqmsg)

        if options.transport == "StompTransport":
            destination = (
                dlqmsg["header"]
                .get("original-destination", dlqmsg["header"]["destination"])
                .split("/", 2)
            )
            if destination[1] == "queue":
                print("sending...")
                send_function = transport.send
            elif destination[1] == "topic":
                print("broadcasting...")
                send_function = transport.broadcast
            else:
                sys.exit("Cannot process message, unknown message mechanism")
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
        elif options.transport == "PikaTransport":
            header = dlqmsg["header"]
            exchange = header.get("headers", {}).get("x-death", {}).get("exchange")
            if exchange:
                import base64
                import urllib

                url = zc.rabbitmqapi["base_url"]
                request = urllib.request.Request(f"{url}/exchanges", method="GET")
                authstring = base64.b64encode(
                    f"{zc.rabbitmqapi['username']}:{zc.rabbitmqapi['password']}".encode()
                ).decode()
                request.add_header("Authorization", f"Basic {authstring}")
                request.add_header("Content-Type", "application/json")
                with urllib.request.urlopen(request) as response:
                    reply = response.read()
                exchange_info = json.loads(reply)
                for exch in exchange_info:
                    if exch["name"] == exchange:
                        if exch["type"] == "fanout":
                            transport.broadcast(
                                options.destination_override or destination,
                                dlqmsg["message"],
                                headers=header,
                            )
            else:
                destination = header.get("headers", {}).get("x-death", {}).get("queue")
                transport.send(
                    options.destination_override or destination,
                    dlqmsg["message"],
                    headers=header,
                )
        if options.remove:
            os.remove(dlqfile)
        print("Done.\n")

    transport.disconnect()
