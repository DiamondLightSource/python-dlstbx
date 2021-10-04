import argparse
import json
import urllib

import workflows.transport
import zocalo.configuration
from zocalo.configuration import Configuration
from zocalo.util.jmxstats import JMXAPI
from zocalo.util.rabbitmq import http_api_request

#
# dlstbx.dlq_check
#   Check number of messages in dead letter queues
#


def check_dlq(zc: Configuration, namespace: str = None) -> dict:
    """Monitor ActiveMQ queue activity."""
    jmx = JMXAPI(zc)
    if namespace:
        namespace = namespace + "."
    else:
        namespace = ""

    result = jmx.org.apache.activemq(
        type="Broker",
        brokerName="localhost",
        destinationType="Queue",
        destinationName="DLQ.%s*" % namespace,
        attribute="QueueSize",
    )
    if result["status"] == 404:
        return {}
    assert result["status"] == 200, result

    def extract_queue_name(namestring):
        namestringdict = {
            component.split("=")[0]: component.split("=", 1)[1]
            for component in namestring.split(",")
            if "=" in component
        }
        return namestringdict.get("destinationName")

    queuedata = {
        extract_queue_name(name): data["QueueSize"]
        for name, data in result["value"].items()
    }
    return queuedata


def check_dlq_rabbitmq(zc: Configuration, namespace: str = None) -> dict:
    _api_request = http_api_request(zc, "/queues")
    with urllib.request.urlopen(_api_request) as response:
        reply = response.read()
    queue_info = json.loads(reply)
    dlq_info = {}
    for q in queue_info:
        if q["name"].startswith("dlq."):
            if (namespace is None or q["vhost"] == namespace) and int(q["messages"]):
                dlq_info[q["name"]] = int(q["messages"])
    return dlq_info


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
    parser = argparse.ArgumentParser("dlstbx.dlq_check [options]")
    parser.add_argument("-?", action="help", help=argparse.SUPPRESS)
    parser.add_argument(
        "-n",
        "--namespace",
        type=str,
        dest="namespace",
        default="",
        help="Restrict check to this namespace",
    )
    parser.add_argument(
        "-t",
        "--transport",
        type=str,
        dest="transport",
        metavar="TRN",
        default=default_transport,
        help="Transport mechanism. Known mechanisms: "
        + ", ".join(workflows.transport.get_known_transports())
        + f" (default: {default_transport})",
    )
    zc.add_command_line_options(parser)
    args = parser.parse_args()

    if args.transport == "StompTransport":
        dlqs = check_dlq(zc, namespace=args.namespace)
        for queue, count in dlqs.items():
            print("DLQ for %s contains %d entries" % (queue.replace("DLQ.", ""), count))
    elif args.transport == "PikaTransport":
        dlqs = check_dlq_rabbitmq(zc, namespace=args.namespace or "zocalo")
        for queue, count in dlqs.items():
            print("DLQ for %s contains %d entries" % (queue.replace("dlq.", ""), count))
    else:
        exit(f"Transport {args.transport} not recognised")
    total = sum(dlqs.values())
    if total:
        exit(f"Total of {total} DLQ messages found")
    else:
        print("No DLQ messages found")
