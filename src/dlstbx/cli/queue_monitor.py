#
# dlstbx.queue_monitor
#   Monitor queue utilization
#

import argparse
import logging
import re
import time

import pandas as pd
import zocalo.configuration
import zocalo.util.jmxstats
from zocalo.util.rabbitmq import RabbitMQAPI

logger = logging.getLogger("dlstbx.queue_monitor")


def get_rabbitmq_stats(rmq: RabbitMQAPI) -> pd.DataFrame:
    stats = pd.json_normalize(rmq.get("queues").json())

    # If there have been no recently published or delivered messages then these fields
    # might not be present
    if "message_stats.publish" not in stats:
        stats["message_stats.publish"] = 0
    if "message_stats.deliver_get" not in stats:
        stats["message_stats.deliver_get"] = 0

    stats["name.prefix"] = stats["name"].str.split(".", 1).str[0]
    stats["dtype"] = "queue"  # RabbitMQ doesn't have the same queue/topic distinction
    return stats.set_index("name")


def get_activemq_queue_and_topic_info() -> pd.DataFrame:
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


AMQ2RMQ = {
    "EnqueueCount": "message_stats.publish",
    "ConsumerCount": "consumers",
    "QueueSize": "messages_ready",
    "InFlightCount": "messages_unacknowledged",
    "DequeueCount": "message_stats.deliver_get",
}


def get_activemq_stats() -> pd.DataFrame:
    stats = pd.DataFrame(
        columns=[
            "name",
            "consumers",
            "messages",
            "messages_ready",
            "messages_unacknowledged",
            "message_stats.publish",
            "message_stats.deliver_get",
        ]
    )
    for dtype, destinations in zip(
        ("queue", "topic"), get_activemq_queue_and_topic_info()
    ):
        for dname, dinfo in destinations.items():
            row = {AMQ2RMQ.get(k, k): dinfo[k] for k in AMQ2RMQ}
            name = dname.replace("uk.ac.diamond.", "u.a.d.").replace("transient.", "t.")
            name = re.sub(
                "([0-9a-f]{8})-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                "\\1-(..)",
                name,
            )
            row["name"] = name
            row["name.prefix"] = name.split(".", 1)[0]
            row["dtype"] = dtype
            stats = stats.append(row, ignore_index=True)

    stats["messages"] = stats["messages_ready"] + stats["messages_unacknowledged"]
    return stats.set_index("name")


def print_stats(stats: pd.DataFrame, transport_prefix: str) -> None:
    """Main display function"""

    # https://activemq.apache.org/how-do-i-find-the-size-of-a-queue
    # Enqueue Count - the total number of messages sent to the queue since the last restart
    # Dequeue Count - the total number of messages removed from the queue (ack’d by consumer) since last restart
    # Inflight Count - the number of messages sent to a consumer session and have not received an ack
    # Dispatch Count - the total number of messages sent to consumer sessions (Dequeue + Inflight)
    # Expired Count - the number of messages that were not delivered because they were expired
    # QueueSize is the total number of messages in the queue/store that have not been ack’d by a consumer.

    # https://www.rabbitmq.com/rabbitmqctl.8.html#list_queues
    # messages_ready - Number of messages ready to be delivered to clients.
    # messages_unacknowledged - Number of messages delivered to clients but not yet acknowledged.messages
    # messages - Sum of ready and unacknowledged messages (queue depth).

    all_stats = stats.fillna(0.0).astype(int, errors="ignore")
    all_stats = all_stats.sort_index().sort_values(by="messages", ascending=False)
    longest = all_stats.reset_index().astype(str).applymap(len).max()

    c_gray = "\x1b[30m"
    c_green = "\x1b[32m"
    c_yellow = "\x1b[33m"
    c_blue = "\x1b[34m"
    c_magenta = "\x1b[35m"
    c_reset = "\x1b[0m"
    c_bold = "\x1b[1m"

    line = (
        "{colour[namespace]}{qname:{longest[name]}}{colour[reset]}  "
        "{colour[input]}{0[change_publish_rate]:{longest[change_publish_rate]}} "
        ">{colour[hold]}[ {filter_zero[messages_ready]:{longest[messages_ready]}} | {colour[listeners]}{filter_zero[consumers]:<{longest[consumers]}}{colour[hold]} | {colour[flight]}{filter_zero[messages_unacknowledged]:<{longest[messages_unacknowledged]}}{colour[hold]} ]"
        "{colour[output]}> {filter_zero[change_deliver_rate]:<{longest[change_deliver_rate]}}{colour[reset]}"
    )
    # line +=  " -- {0[relevance]}{colour[reset]}"

    print("\033[H\033[J", end="")

    queue_stats = all_stats[all_stats["dtype"] == "queue"]
    topic_stats = all_stats[all_stats["dtype"] == "topic"]

    queue_sep = "{header}{transport_prefix} status: {highlight}{queues}{header} queues containing {highlight}{messages}{header} messages{reset}".format(
        messages=queue_stats["messages_ready"].sum(),
        queues=len(queue_stats),
        highlight=c_bold + c_yellow,
        reset=c_reset,
        header=c_reset + c_yellow,
        transport_prefix=transport_prefix,
    )
    topic_sep = (
        "\n{header}ActiveMQ status: {highlight}{topics}{header} topics{reset}".format(
            topics=len(topic_stats),
            highlight=c_bold + c_yellow,
            reset=c_reset,
            header=c_reset + c_yellow,
        )
    )

    for sep, stats in ((queue_sep, queue_stats), (topic_sep, topic_stats)):
        if not len(stats):
            continue

        print(sep)
        stats = stats[stats["relevance"] > 0]
        status = stats.to_dict(orient="index")

        for qname in status.keys():
            colour = {
                "input": c_green if status[qname]["change_publish_rate"] else c_gray,
                "hold": c_blue if status[qname]["messages_ready"] else c_gray,
                "flight": c_blue
                if status[qname]["messages_ready"]
                or status[qname]["messages_unacknowledged"]
                else c_gray,
                "output": c_green if status[qname]["change_deliver_rate"] else c_gray,
                "reset": c_reset,
                "listeners": c_yellow if status[qname]["consumers"] else c_gray,
                "namespace": c_magenta
                if status[qname]["name.prefix"] == "zocdev"
                else "",
            }
            filter_zero = {
                key: status[qname][key] if status[qname][key] > 0 else ""
                for key in (
                    "change_deliver_rate",
                    "messages_unacknowledged",
                    "messages_ready",
                    "consumers",
                )
            }
            print(
                line.format(
                    status[qname],
                    qname=qname,
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
    parser = argparse.ArgumentParser(usage="dlstbx.queue_monitor")
    parser.add_argument("-?", action="help", help=argparse.SUPPRESS)
    parser.add_argument(
        "--rabbitmq",
        action="store_true",
        dest="rabbitmq",
        help="Show stats for the RabbitMQ server",
    )
    parser.add_argument(
        "--interval",
        dest="gather_interval",
        default=5,
        help="Interval (in seconds) at which to gather statistics",
    )

    # Load configuration
    zc = zocalo.configuration.from_file()
    zc.activate()
    zc.add_command_line_options(parser)

    args = parser.parse_args()

    previous_stats = None

    if not args.rabbitmq:
        global jmx
        jmx = zocalo.util.jmxstats.JMXAPI(zc)
        transport_prefix = "ActiveMQ"
    else:
        rmq = RabbitMQAPI.from_zocalo_configuration(zc)
        transport_prefix = "RabbitMQ"

    try:
        while True:
            if args.rabbitmq:
                stats = get_rabbitmq_stats(rmq)
            else:
                stats = get_activemq_stats()

            stats["relevance"] = (
                stats["messages_ready"] + stats["messages_unacknowledged"]
            )
            if previous_stats is not None:
                stats["change_publish_rate"] = (
                    stats["message_stats.publish"]
                    - previous_stats["message_stats.publish"]
                )
                stats["change_deliver_rate"] = (
                    stats["message_stats.deliver_get"]
                    - previous_stats["message_stats.deliver_get"]
                )
                stats["changehist_publish_rate"] = stats[
                    "change_publish_rate"
                ] - previous_stats.get("change_publish_rate", 0)
                change_hist_publish_rate = stats[
                    "change_publish_rate"
                ] - previous_stats.get("change_publish_rate", 0)
                stats["relevance"] = stats["relevance"].add(
                    previous_stats["relevance"] // 2
                    + previous_stats["change_publish_rate"].clip(lower=0)
                    + previous_stats["change_deliver_rate"].clip(lower=0)
                    + change_hist_publish_rate.clip(lower=0)
                )
            else:
                stats["change_publish_rate"] = 0
                stats["change_deliver_rate"] = 0
                stats["changehist_publish_rate"] = 0
            print_stats(stats, transport_prefix=transport_prefix)
            previous_stats = stats
            time.sleep(args.gather_interval)
    except KeyboardInterrupt:
        print("\x1b[0m")
