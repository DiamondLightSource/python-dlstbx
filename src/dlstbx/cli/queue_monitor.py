#
# dlstbx.queue_monitor
#   Monitor queue utilization
#

import getpass
import logging
import re
import time
from optparse import SUPPRESS_HELP, OptionParser

import dlstbx.util.jmxstats

logger = logging.getLogger("dlstbx.queue_monitor")


class QueueStatus:
    """Monitor ActiveMQ queue activity."""

    gather_interval = 5
    status = {}

    def run(self):
        """Obtain statistics and print status. In a loop."""
        while True:
            self.update_status()
            self.print_status()
            time.sleep(self.gather_interval)

    def get_queue_and_topic_info(self):
        attributes = [
            "ConsumerCount",
            "QueueSize",
            "EnqueueCount",
            "DequeueCount",
            "InFlightCount",
        ]
        queues = jmx.org.apache.activemq(
            type="Broker",
            brokerName="localhost",
            destinationType="Queue",
            destinationName="*",
        )
        queue_info, topic_info = {}, {}
        if "value" in queues:
            for destination in queues["value"]:
                dest = destination[destination.index("destinationName=") :]
                dest = (dest.split(",")[0])[16:]
                if dest.startswith("ActiveMQ.Advisory."):
                    continue
                queue_info[dest] = queues["value"][destination]
        else:
            logger.warning("Could not obtain queue status via JMX.\n%r", queues)
        topics = jmx.org.apache.activemq(
            type="Broker",
            brokerName="localhost",
            destinationType="Topic",
            destinationName="*",
            attribute=",".join(attributes),
        )
        if "value" in topics:
            for destination in topics["value"]:
                dest = destination[destination.index("destinationName=") :]
                dest = (dest.split(",")[0])[16:]
                if dest.startswith("ActiveMQ.Advisory."):
                    continue
                topic_info[dest] = topics["value"][destination]
        else:
            logger.warning("Could not obtain topic status via JMX.\n%r", topics)
        return queue_info, topic_info

    def update_status(self):
        previous = self.status
        self.status = {"queue": {}, "topic": {}}
        for dtype, destinations in zip(
            ("queue", "topic"), self.get_queue_and_topic_info()
        ):
            for dname, dinfo in destinations.items():
                self.status[dtype][dname] = dinfo
                shortdest = dname.replace("uk.ac.diamond.", "u.a.d.").replace(
                    "transient.", "t."
                )
                shortdest = re.sub(
                    "([0-9a-f]{8})-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                    "\\1-(..)",
                    shortdest,
                )
                dinfo["shortdest"] = shortdest
                dinfo["shortdest.prefix"] = shortdest.split(".", 1)[0]
                dinfo["relevance"] = dinfo["QueueSize"] + dinfo["InFlightCount"]

                last_status = previous.get(dtype, {}).get(dname, {})
                # Find change
                for key in (
                    "ConsumerCount",
                    "QueueSize",
                    "EnqueueCount",
                    "DequeueCount",
                    "InFlightCount",
                ):
                    dinfo["change-" + key] = dinfo.get(key, 0) - last_status.get(
                        key, dinfo.get(key, 0)
                    )

                    # Find change trend (2nd derivative)
                    dinfo["changehist-" + key] = dinfo[
                        "change-" + key
                    ] - last_status.get("change-" + key, dinfo["change-" + key])

                if last_status:
                    dinfo["relevance"] += (
                        last_status["relevance"] // 2
                        + max(0, dinfo["change-EnqueueCount"])
                        + max(0, dinfo["changehist-EnqueueCount"])
                        + max(0, dinfo["change-DequeueCount"])
                    )

    def print_status(self):
        """Main display function"""

        longest = {}
        for key in (
            "shortdest",
            "change-EnqueueCount",
            "ConsumerCount",
            "QueueSize",
            "InFlightCount",
            "change-DequeueCount",
        ):
            longest[key] = max(
                len(str(self.status[dtype][d][key]))
                for dtype in ("queue", "topic")
                for d in self.status[dtype]
            )

        c_gray = "\x1b[30m"
        c_green = "\x1b[32m"
        c_yellow = "\x1b[33m"
        c_blue = "\x1b[34m"
        c_magenta = "\x1b[35m"
        c_reset = "\x1b[0m"
        c_bold = "\x1b[1m"

        line = (
            "{colour[namespace]}{0[shortdest]:{longest[shortdest]}}{colour[reset]}  "
            "{colour[input]}{0[change-EnqueueCount]:{longest[change-EnqueueCount]}} "
            ">{colour[hold]}[ {filter_zero[QueueSize]:{longest[QueueSize]}} | {colour[listeners]}{filter_zero[ConsumerCount]:<{longest[ConsumerCount]}}{colour[hold]} | {colour[flight]}{filter_zero[InFlightCount]:<{longest[InFlightCount]}}{colour[hold]} ]"
            "{colour[output]}> {filter_zero[change-DequeueCount]:<{longest[change-DequeueCount]}}{colour[reset]}"
        )
        #   line +=  " -- {0[relevance]}{colour[reset]}"

        print("\033[H\033[J", end="")
        queue_sep = "{header}ActiveMQ status: {highlight}{queues}{header} queues containing {highlight}{messages}{header} messages{reset}".format(
            messages=sum(q["QueueSize"] for q in self.status["queue"].values()),
            queues=len(self.status["queue"]),
            highlight=c_bold + c_yellow,
            reset=c_reset,
            header=c_reset + c_yellow,
        )
        topic_sep = "\n{header}ActiveMQ status: {highlight}{topics}{header} topics{reset}".format(
            topics=len(self.status["topic"]),
            highlight=c_bold + c_yellow,
            reset=c_reset,
            header=c_reset + c_yellow,
        )

        for dtype, header in (("queue", queue_sep), ("topic", topic_sep)):
            print(header)
            destinations = [
                d for d in self.status[dtype] if self.status[dtype][d]["relevance"] > 0
            ]
            destinations.sort(key=lambda d: self.status[dtype][d]["shortdest"])
            destinations.sort(
                key=lambda d: self.status[dtype][d]["relevance"], reverse=True
            )

            for dname in destinations:
                colour = {
                    "input": c_green
                    if self.status[dtype][dname]["change-EnqueueCount"]
                    else c_gray,
                    "hold": c_blue
                    if self.status[dtype][dname]["QueueSize"]
                    else c_gray,
                    "flight": c_blue
                    if self.status[dtype][dname]["QueueSize"]
                    or self.status[dtype][dname]["InFlightCount"]
                    else c_gray,
                    "output": c_green
                    if self.status[dtype][dname]["change-DequeueCount"]
                    else c_gray,
                    "reset": c_reset,
                    "listeners": c_yellow
                    if self.status[dtype][dname]["ConsumerCount"]
                    else c_gray,
                    "namespace": c_magenta
                    if self.status[dtype][dname]["shortdest.prefix"] == "zocdev"
                    else "",
                }
                filter_zero = {
                    key: self.status[dtype][dname][key]
                    if self.status[dtype][dname][key] > 0
                    else ""
                    for key in (
                        "change-DequeueCount",
                        "InFlightCount",
                        "QueueSize",
                        "ConsumerCount",
                    )
                }
                print(
                    line.format(
                        self.status[dtype][dname],
                        longest=longest,
                        colour=colour,
                        filter_zero=filter_zero,
                    )
                )

        print(
            "\n{header}What do the numbers mean:{reset}".format(
                reset=c_reset,
                header=c_reset + c_yellow,
            )
        )
        print(
            f"topic/queue name  {c_green}m.in/5s >{c_gray}[ {c_blue}m.held{c_gray} | {c_yellow}clients{c_gray} | {c_blue}m.dispatchd{c_gray} ]{c_green}> m.out/5s{c_reset}"
        )


def run():
    parser = OptionParser(usage="dlstbx.queue_monitor")
    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option(
        "--test",
        action="store_true",
        dest="test",
        help="Connect to personal development ActiveMQ server",
    )

    (options, args) = parser.parse_args()

    global jmx
    if options.test:
        jmx = dlstbx.util.jmxstats.JMXAPI(
            "/dls/tmp/%s/zocdev-activemq/latest-credentials" % getpass.getuser()
        )
    else:
        jmx = dlstbx.util.jmxstats.JMXAPI()

    try:
        QueueStatus().run()
    except KeyboardInterrupt:
        print("\x1b[0m")
