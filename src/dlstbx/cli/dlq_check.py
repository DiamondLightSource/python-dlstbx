import sys
from optparse import SUPPRESS_HELP, OptionParser

import dlstbx.util.jmxstats

#
# dlstbx.dlq_check
#   Check number of messages in dead letter queues
#


def check_dlq(namespace=None):
    """Monitor ActiveMQ queue activity."""

    jmx = dlstbx.util.jmxstats.JMXAPI()
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


def run():
    parser = OptionParser(usage="dlstbx.dlq_check [options]")
    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option(
        "-n",
        "--namespace",
        dest="namespace",
        default="",
        help="Restrict check to this namespace",
    )
    (options, args) = parser.parse_args()

    dlqs = check_dlq(namespace=options.namespace)
    for queue, count in dlqs.items():
        print("DLQ for %s contains %d entries" % (queue.replace("DLQ.", ""), count))
    total = sum(dlqs.values())
    if total:
        print("Total of %d DLQ messages found" % total)
        sys.exit(1)
    else:
        print("No DLQ messages found")
