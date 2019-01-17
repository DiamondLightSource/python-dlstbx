#
# dlstbx.gadabout
#   GDA event interface
#

from __future__ import absolute_import, division, print_function

import argparse
import getpass
import os
import socket
import sys
from pprint import pprint

import workflows
from workflows.transport.stomp_transport import StompTransport

# Example: dlstbx.gadabout --event=START 12345

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Records a GDA data collection event in Zocalo.",
        prog="dlstbx.gadabout",
    )

    parser.add_argument("event")
    parser.add_argument("dcid", type=int)
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        action="store_true",
        default=False,
        help="Show raw message before sending",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        dest="test",
        default=False,
        help="Run in ActiveMQ testing (zocdev) namespace",
    )
    default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-live.cfg"
    allow_stomp_fallback = not any("stomp" in s.lower() for s in sys.argv)
    if "--test" in sys.argv:
        # override default stomp host
        default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-testing.cfg"
        allow_stomp_fallback = False
    try:
        StompTransport.load_configuration_file(default_configuration)
    except workflows.Error as e:
        print("Error: %s\n" % str(e))
        allow_stomp_fallback = False
    StompTransport.add_command_line_options(parser)
    args = parser.parse_args(sys.argv[1:])

    # Pickup command can't deal with Gadabout messages, so...
    allow_stomp_fallback = False

    def write_message_to_dropfile(message, headers):
        import json
        import uuid

        # Instruct pickup process to send message to separate queue
        headers["pickup.queue"] = "reduce.gadabout"
        message_serialized = (
            json.dumps({"headers": headers, "message": message}, indent=2) + "\n"
        )
        fallback = os.path.join("/dls_sw/apps/zocalo/dropfiles", str(uuid.uuid4()))
        with open(fallback, "w") as fh:
            fh.write(message_serialized)
        print("Message successfully stored in %s" % fallback)

    def send_to_stomp_or_defer(message, headers=None):
        if not headers:
            headers = {}
        if args.verbose:
            pprint(message)
        if allow_stomp_fallback and options.dropfile:
            return write_message_to_dropfile(message, headers)
        try:
            stomp = StompTransport()
            stomp.connect()
            stomp.send("reduce.gadabout", message, headers=headers)
        except (
            KeyboardInterrupt,
            SyntaxError,
            AssertionError,
            AttributeError,
            ImportError,
            TypeError,
            ValueError,
        ):
            raise
        except Exception:
            if not allow_stomp_fallback:
                raise
            print("\n\n")
            import traceback

            traceback.print_exc()
            print("\n\nAttempting to store message in fallback location")
            write_message_to_dropfile(message, headers)

    assert args.dcid > 0, "Invalid data collection ID given."

    message = {
        "message": "event",
        "event": args.event,
        "dcid": args.dcid,
        "source.user": getpass.getuser(),
        "source.host": socket.gethostname(),
    }

    send_to_stomp_or_defer(message)
    print("\nEvent {0.event} recorded for DCID {0.dcid}.".format(args))
