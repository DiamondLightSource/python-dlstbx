#
# dlstbx.dlq_purge
#   Retrieve all dead letter queue messages from ActiveMQ and store them
#   in a temporary directory.
#


import errno
import json
import os
import queue
import re
import sys
import time
from optparse import SUPPRESS_HELP, OptionParser

import workflows
import zocalo.configuration

# from workflows.transport.stomp_transport import StompTransport


def run():
    zc = zocalo.configuration.from_file()
    zc.activate()
    default_transport = workflows.transport.default_transport
    if (
        zc.storage
        and zc.storage.get("zocalo.default_transport")
        in workflows.transport.get_known_transports()
    ):
        default_transport = zc.storage["zocalo.default_transport"]
    parser = OptionParser(usage="dlstbx.dlq_purge [options] [queue [queue ...]]")

    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option(
        "--test",
        action="store_true",
        dest="test",
        help="Run in ActiveMQ testing (zocdev) namespace",
    )
    dlqprefix = "zocalo"
    if "--test" in sys.argv:
        dlqprefix = "zocdev"
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
        + " (default: %default)",
    )

    workflows.transport.add_command_line_options(parser)
    (options, args) = parser.parse_args(["--stomp-prfx=DLQ"] + sys.argv[1:])
    if options.transport == "PikaTransport":
        args = ["dlq." + a for a in args if a != "--stomp-prfx=DLQ"]
    transport = workflows.transport.lookup(options.transport)()

    characterfilter = re.compile(r"[^a-zA-Z0-9._-]+", re.UNICODE)
    idlequeue = queue.Queue()

    def receive_dlq_message(header, message):
        idlequeue.put_nowait("start")
        timestamp = time.localtime(int(header["timestamp"]) / 1000)
        millisec = int(header["timestamp"]) % 1000
        filepath = os.path.join(
            "/dls/tmp/zocalo/DLQ",
            time.strftime("%Y-%m-%d", timestamp),
            #       time.strftime('%H-%M', timestamp),
        )
        filename = (
            "msg-"
            + time.strftime("%Y%m%d-%H%M%S", timestamp)
            + "-"
            + "%03d" % millisec
            + "-"
            + characterfilter.sub("_", header["message-id"])
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
        transport.ack(header)
        idlequeue.put_nowait("done")

    transport.connect()
    if not args:
        args = [dlqprefix + ".>"]
    for queue_ in args:
        print("Looking for DLQ messages in " + queue_)
        transport.subscribe(queue_, receive_dlq_message, acknowledgement=True)
    try:
        idlequeue.get(True, options.wait or 3)
        while True:
            idlequeue.get(True, options.wait or 0.1)
    except queue.Empty:
        print("Done.")
