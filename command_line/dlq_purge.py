#
# dlstbx.dlq_purge
#   Retrieve all dead letter queue messages from ActiveMQ and store them
#   in a temporary directory.
#


import errno
import json
import os
import re
import sys
import time
from optparse import SUPPRESS_HELP, OptionParser

from six.moves import queue
from workflows.transport.stomp_transport import StompTransport

if __name__ == "__main__":
    parser = OptionParser(usage="dlstbx.dlq_purge [options] [queue [queue ...]]")

    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option(
        "--test",
        action="store_true",
        dest="test",
        help="Run in ActiveMQ testing (zocdev) namespace",
    )
    default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-live.cfg"
    dlqprefix = "zocalo"
    if "--test" in sys.argv:
        default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-testing.cfg"
        dlqprefix = "zocdev"
    # override default stomp host
    parser.add_option(
        "--wait",
        action="store",
        dest="wait",
        type=float,
        help="Wait this many seconds for ActiveMQ replies",
    )
    StompTransport.load_configuration_file(default_configuration)

    StompTransport.add_command_line_options(parser)
    (options, args) = parser.parse_args(["--stomp-prfx=DLQ"] + sys.argv[1:])
    stomp = StompTransport()

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
        stomp.ack(header)
        idlequeue.put_nowait("done")

    stomp.connect()
    if not args:
        args = [dlqprefix + ".>"]
    for queue_ in args:
        print("Looking for DLQ messages in " + queue_)
        stomp.subscribe(queue_, receive_dlq_message, acknowledgement=True)
    try:
        idlequeue.get(True, options.wait or 3)
        while True:
            idlequeue.get(True, options.wait or 0.1)
    except queue.Empty:
        print("Done.")
