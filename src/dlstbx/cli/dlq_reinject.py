#
# dlstbx.dlq_reinject
#   Take a dead letter queue message from a file and send it back to its queue
#   for a retry.
#


import argparse
import json
import os
import re
import select
import sys
import time
from pprint import pprint

import workflows.transport
import zocalo.configuration
from zocalo.util.rabbitmq import http_api_request


def run() -> None:
    zc = zocalo.configuration.from_file()
    zc.activate()
    default_transport = workflows.transport.default_transport
    parser = argparse.ArgumentParser(
        usage="dlstbx.dlq_reinject [options] file [file [..]]"
    )

    parser.add_argument("-?", action="help", help=argparse.SUPPRESS)
    parser.add_argument(
        "-r",
        "--remove",
        action="store_true",
        default=False,
        dest="remove",
        help="Delete file on successful reinjection",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        dest="verbose",
        help="Show message contents",
    )
    parser.add_argument(
        "-d",
        "--destination",
        action="store",
        default=None,
        dest="destination_override",
        help="Reinject messages to a different destination. Any name given must include the stomp prefix.",
    )
    parser.add_argument(
        "-w",
        "--wait",
        default=None,
        dest="wait",
        help="Wait this many seconds between reinjections",
    )
    parser.add_argument(
        "-t",
        "--transport",
        dest="transport",
        metavar="TRN",
        default=default_transport,
        help="Transport mechanism. Known mechanisms: "
        + ", ".join(workflows.transport.get_known_transports())
        + f" (default: {default_transport})",
    )

    zc.add_command_line_options(parser)
    workflows.transport.add_command_line_options(parser)
    (known_args, unknown_args) = parser.parse_known_args()
    transport = workflows.transport.lookup(known_args.transport)()

    stdin = []
    if select.select([sys.stdin], [], [], 0.0)[0]:
        dlq_purge_filename_format = re.compile(r"^  \/")
        while True:
            line = sys.stdin.readline()
            if not line:
                break
            if dlq_purge_filename_format.match(line):
                stdin.append(line.strip())
        print(f"{len(stdin)} filenames read from stdin")

    if not unknown_args and not stdin:
        print("No DLQ message files given.")
        sys.exit(0)

    transport.connect()

    first = True
    for dlqfile in unknown_args + stdin:
        if not os.path.exists(dlqfile):
            print(f"Ignoring missing file {dlqfile}")
            continue
        if not first and known_args.wait:
            time.sleep(float(known_args.wait))
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
        if known_args.verbose:
            pprint(dlqmsg)

        if known_args.transport == "StompTransport":
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
            if known_args.destination_override:
                destination[2] = known_args.destination_override
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
        elif known_args.transport == "PikaTransport":
            header = dlqmsg["header"]
            exchange = header.get("headers", {}).get("x-death", {})[0].get("exchange")
            if exchange:
                import urllib

                _api_request = http_api_request(zc, "/queues")
                with urllib.request.urlopen(_api_request) as response:
                    reply = response.read()
                exchange_info = json.loads(reply)
                for exch in exchange_info:
                    if exch["name"] == exchange:
                        if exch["type"] == "fanout":
                            header = _rabbit_prepare_header(header)
                            transport.broadcast(
                                known_args.destination_override or destination,
                                dlqmsg["message"],
                                headers=header,
                            )
            else:
                destination = (
                    header.get("headers", {}).get("x-death", {})[0].get("queue")
                )
                header = _rabbit_prepare_header(header)
                transport.send(
                    known_args.destination_override or destination,
                    dlqmsg["message"],
                    headers=header,
                )
        if known_args.remove:
            os.remove(dlqfile)
        print("Done.\n")

    transport.disconnect()


def _rabbit_prepare_header(header: dict) -> dict:
    drop = (
        "message-id",
        "routing_key",
        "redelivered",
        "exchange",
        "consumer_tag",
        "delivery_mode",
    )
    return {k: str(v) for k, v in header.items() if k not in drop}
