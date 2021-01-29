#
# dlstbx.queue_drain
#   Drain one queue into another in a controlled manner
#


import queue
import sys
import time
from datetime import datetime
from optparse import SUPPRESS_HELP, OptionParser

from workflows.transport.stomp_transport import StompTransport
import workflows.recipe.wrapper


def show_cluster_info(step):
    try:
        print("Beamline " + step["parameters"]["cluster_project"].upper())
    except Exception:
        pass
    try:
        print("Working directory " + step["parameters"]["workingdir"])
    except Exception:
        pass


show_additional_info = {"cluster.submission": show_cluster_info}


def run():
    parser = OptionParser(usage="dlstbx.queue_drain [options] source destination")

    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option(
        "--test",
        action="store_true",
        dest="test",
        help="Run in ActiveMQ testing (zocdev) namespace",
    )
    default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-live.cfg"
    if "--test" in sys.argv:
        default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-testing.cfg"
    # override default stomp host
    parser.add_option(
        "--wait",
        action="store",
        dest="wait",
        type=float,
        default=5,
        help="Wait this many seconds between deliveries",
    )
    parser.add_option(
        "--stop",
        action="store",
        dest="stop",
        type=float,
        default=60,
        help="Stop if no message seen for this many seconds (0 = forever)",
    )
    StompTransport.load_configuration_file(default_configuration)

    StompTransport.add_command_line_options(parser)
    options, args = parser.parse_args()
    stomp = StompTransport()

    if len(args) != 2:
        sys.exit("You must specify one source and one destination queue")
    q_source, q_dest = args

    messages = queue.Queue()

    def receive_message(header, message):
        messages.put((header, message))

    stomp.connect()
    print("Reading messages from " + q_source)
    stomp.subscribe(q_source, receive_message, acknowledgement=True)

    message_count = 0
    header_filter = frozenset(
        {
            "content-length",
            "destination",
            "expires",
            "message-id",
            "original-destination",
            "originalExpiration",
            "subscription",
            "timestamp",
            "redelivered",
        }
    )
    drain_start = time.time()
    idle_time = 0
    try:
        while True:
            try:
                header, message = messages.get(True, 0.1)
            except queue.Empty:
                idle_time = idle_time + 0.1
                if options.stop and idle_time > options.stop:
                    break
                continue
            idle_time = 0
            print()
            try:
                print(
                    "Message date: {:%Y-%m-%d %H:%M:%S}".format(
                        datetime.fromtimestamp(int(header["timestamp"]) / 1000)
                    )
                )
            except Exception:
                pass
            try:
                print("Recipe ID:    {}".format(message["environment"]["ID"]))
                r = workflows.recipe.wrapper.RecipeWrapper(message=message)
                show_additional_info.get(
                    q_dest, show_additional_info.get(r.recipe_step["queue"])
                )(r.recipe_step)
            except Exception:
                pass

            new_headers = {
                key: header[key] for key in header if key not in header_filter
            }
            txn = stomp.transaction_begin()
            stomp.send(q_dest, message, headers=new_headers, transaction=txn)
            stomp.ack(header, transaction=txn)
            stomp.transaction_commit(txn)
            message_count = message_count + 1
            print(
                "%4d message(s) drained in %.1f seconds"
                % (message_count, time.time() - drain_start)
            )
            time.sleep(options.wait)
    except KeyboardInterrupt:
        sys.exit(
            "\nCancelling, %d message(s) drained, %d message(s) unprocessed in memory"
            % (message_count, messages.qsize())
        )
    print(
        "%d message(s) drained, no message seen for %.1f seconds"
        % (message_count, idle_time)
    )
