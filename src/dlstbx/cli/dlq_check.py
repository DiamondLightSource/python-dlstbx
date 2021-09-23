import base64
import json
import sys
import urllib
from optparse import SUPPRESS_HELP, OptionParser

import zocalo.configuration
from zocalo.util.jmxstats import JMXAPI

#
# dlstbx.dlq_check
#   Check number of messages in dead letter queues
#


def check_dlq(zc, namespace=None):
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


def check_dlq_rabbitmq(zc, namespace=None):
    url = zc.rabbitmqapi["base_url"]
    request = urllib.request.Request(f"{url}/queues", method="GET")
    authstring = base64.b64encode(
        f"{zc.rabbitmqapi['username']}:{zc.rabbitmqapi['password']}".encode()
    ).decode()
    request.add_header("Authorization", f"Basic {authstring}")
    request.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(request) as response:
        reply = response.read()
    queue_info = json.loads(reply)
    dlq_info = {}
    for q in queue_info:
        if q["name"].startswith("dlq."):
            if namespace is None or q["vhost"] == namespace:
                dlq_info[q["name"]] = int(q["messages"])
    return dlq_info


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
    parser.add_option(
        "-r",
        "--rabbitmq",
        dest="rabbit",
        action="store_true",
        help="Check rabbitmq dead letter queues",
    )
    parser.add_option(
        "--jmx-test-creds",
        dest="jmx_creds",
        default=None,
        help="Config file containing JMX credentials",
    )
    zc = zocalo.configuration.from_file()
    zc.activate()
    zc.add_command_line_options(parser)
    (options, args) = parser.parse_args()
    if options.jmx_creds is not None:
        import configparser

        config = configparser.ConfigParser()
        config.read(options.jmx_creds)
        setattr(
            zc,
            "jmx",
            {
                "host": config["jmx"]["host"],
                "port": config["jmx"]["port"],
                "base_url": config["jmx"]["baseurl"],
                "username": config["jmx"]["username"],
                "password": config["jmx"]["password"],
            },
        )

    if not options.rabbit:
        dlqs = check_dlq(zc, namespace=options.namespace)
        for queue, count in dlqs.items():
            print("DLQ for %s contains %d entries" % (queue.replace("DLQ.", ""), count))
    else:
        dlqs = check_dlq_rabbitmq(zc, namespace=options.namespace)
        for queue, count in dlqs.items():
            print("DLQ for %s contains %d entries" % (queue.replace("dlq.", ""), count))
    total = sum(dlqs.values())
    if total:
        print("Total of %d DLQ messages found" % total)
        sys.exit(1)
    else:
        print("No DLQ messages found")
