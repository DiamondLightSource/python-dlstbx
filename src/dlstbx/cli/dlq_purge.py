#
# dlstbx.dlq_purge
#   Retrieve all dead letter queue messages from ActiveMQ and store them
#   in a temporary directory.
#


import argparse
import errno
import json
import os
import queue
import re
import sys
import time
from datetime import datetime
from functools import partial

import workflows
import zocalo.configuration

# from workflows.transport.stomp_transport import StompTransport


def run() -> None:
    zc = zocalo.configuration.from_file()
    zc.activate()
    default_transport = workflows.transport.default_transport
    if (
        zc.storage
        and zc.storage.get("zocalo.default_transport")
        in workflows.transport.get_known_transports()
    ):
        default_transport = zc.storage["zocalo.default_transport"]
    parser = argparse.ArgumentParser(
        usage="dlstbx.dlq_purge [options] [queue [queue ...]]"
    )

    parser.add_option("-?", action="help", help=argparse.SUPPRESS)
    dlqprefix = "zocalo"
    # override default stomp host
    parser.add_option(
        "--wait",
        action="store",
        dest="wait",
        type=float,
        help="Wait this many seconds for ActiveMQ replies",
    )
    parser.add_option(
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
    (options, args) = parser.parse_args(["--stomp-prfx=DLQ"] + sys.argv[1:])
    if options.transport == "PikaTransport":
        args = ["dlq." + a for a in args]
    transport = workflows.transport.lookup(options.transport)()

    if zc.storage and zc.storage.get("zocalo.dlq.purge_location"):
        dlq_dump_path = zc.storage["zocalo.dlq.purge_location"]
    else:
        dlq_dump_path = "./DLQ"

    characterfilter = re.compile(r"[^a-zA-Z0-9._-]+", re.UNICODE)
    idlequeue = queue.Queue()

    def receive_dlq_message(header, message, rabbitmq=False):
        idlequeue.put_nowait("start")
        if rabbitmq:
            msg_time = int(datetime.timestamp(header["headers"]["x-death"][0]["time"]))
            header["headers"]["x-death"][0]["time"] = datetime.timestamp(
                header["headers"]["x-death"][0]["time"]
            )
        else:
            msg_time = int(header["timestamp"])
        timestamp = time.localtime(msg_time / 1000)
        millisec = msg_time % 1000
        filepath = os.path.join(
            dlq_dump_path,
            time.strftime("%Y-%m-%d", timestamp),
            #       time.strftime('%H-%M', timestamp),
        )
        filename = (
            "msg-"
            + time.strftime("%Y%m%d-%H%M%S", timestamp)
            + "-"
            + "%03d" % millisec
            + "-"
            + characterfilter.sub("_", str(header["message-id"]))
        )
        try:
            os.makedirs(filepath)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(filepath):
                pass
            else:
                raise

        dlqmsg = {
            "exported": {
                "date": time.strftime("%Y-%m-%d"),
                "time": time.strftime("%H:%M:%S"),
            },
            "header": header,
            "message": message,
        }

        with open(os.path.join(filepath, filename), "w") as fh:
            fh.write(json.dumps(dlqmsg, indent=2, sort_keys=True))
        print(
            "Message {id} ({timestamp}) exported:\n  {filename}".format(
                id=header["message-id"],
                timestamp=time.strftime("%Y-%m-%d %H:%M:%S", timestamp),
                filename=os.path.join(filepath, filename),
            )
        )
        if rabbitmq:
            # subscription_id does nothing for RabbitMQ but it is currently required by workflows
            transport.ack(header, subscription_id=header["message-id"])
        else:
            transport.ack(header)
        idlequeue.put_nowait("done")

    transport.connect()
    if not args:
        args = [dlqprefix + ".>"]
    for queue_ in args:
        print("Looking for DLQ messages in " + queue_)
        if options.transport == "PikaTransport":
            transport.subscribe(
                queue_,
                partial(receive_dlq_message, rabbitmq=True),
                acknowledgement=True,
            )
        else:
            transport.subscribe(queue_, receive_dlq_message, acknowledgement=True)
    try:
        idlequeue.get(True, options.wait or 3)
        while True:
            idlequeue.get(True, options.wait or 0.1)
    except queue.Empty:
        print("Done.")
