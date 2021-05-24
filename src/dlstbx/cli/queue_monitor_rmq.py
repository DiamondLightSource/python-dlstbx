#
# dlstbx.queue_monitor
#   Monitor queue utilization
#

import argparse
import logging
import time

import json
import numpy as np
import pandas as pd
import urllib.parse
import urllib.request

logger = logging.getLogger("dlstbx.queue_monitor")


RABBITMQ_HOST = "rabbitmq1.diamond.ac.uk"


def get_queue_stats() -> pd.DataFrame:
    password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    password_mgr.add_password(
        realm=None,
        uri=f"http://{RABBITMQ_HOST}:15672/api/",
        user="guest",
        passwd="guest",
    )
    handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
    opener = urllib.request.build_opener(handler)
    urllib.request.install_opener(opener)
    request = urllib.request.Request(f"http://{RABBITMQ_HOST}:15672/api/queues")
    with urllib.request.urlopen(request) as response:
        json_str = response.read()
    stats = pd.json_normalize(json.loads(json_str))
    fields = [
        "name",
        "consumers",
        "messages",
        "messages_ready",
        "messages_unacknowledged",
        "message_stats.publish",
        "message_stats.publish_details.rate",
        "message_stats.deliver_get",
        "message_stats.deliver_get_details.rate",
    ]
    return stats[fields]


def print_stats(stats: pd.DataFrame) -> None:
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

    # Translate to ActiveMQ field names
    stats = stats.rename(
        columns={
            "name": "shortdest",
            "consumers": "ConsumerCount",
            "messages_unacknowledged": "InFlightCount",
            "messages_ready": "QueueSize",
        }
    )

    # Convert rates to equivalent number of messages in last 5 seconds
    stats["change-EnqueueCount"] = (
        stats["message_stats.publish_details.rate"] * 5
    ).apply(np.ceil)
    stats["change-DequeueCount"] = (
        stats["message_stats.deliver_get_details.rate"] * 5
    ).apply(np.ceil)

    stats = stats.fillna(0.0).astype(int, errors="ignore")
    stats["name"] = stats["shortdest"]
    stats = stats[stats["messages"] > 0].set_index("name")
    stats = stats.sort_index().sort_values(by="messages")
    status = stats.to_dict(orient="index")
    longest = stats.astype(str).applymap(len).max()

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
        messages=stats["QueueSize"].sum(),
        queues=len(stats),
        highlight=c_bold + c_yellow,
        reset=c_reset,
        header=c_reset + c_yellow,
    )
    print(queue_sep)

    for dname in status.keys():
        colour = {
            "input": c_green if status[dname]["change-EnqueueCount"] else c_gray,
            "hold": c_blue if status[dname]["QueueSize"] else c_gray,
            "flight": c_blue
            if status[dname]["QueueSize"] or status[dname]["InFlightCount"]
            else c_gray,
            "output": c_green if status[dname]["change-DequeueCount"] else c_gray,
            "reset": c_reset,
            "listeners": c_yellow if status[dname]["ConsumerCount"] else c_gray,
            "namespace": c_magenta
            # if status[dname]["shortdest.prefix"] == "zocdev"
            # else "",
        }
    filter_zero = {
        key: status[dname][key] if status[dname][key] > 0 else ""
        for key in (
            "change-DequeueCount",
            "InFlightCount",
            "QueueSize",
            "ConsumerCount",
        )
    }
    print(
        line.format(
            status[dname],
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
        "--test",
        action="store_true",
        dest="test",
        help="Connect to personal development ActiveMQ server",
    )
    parser.add_argument(
        "--interval",
        dest="gather_interval",
        default=5,
        help="Interval (in seconds) at which to gather statistics",
    )

    args = parser.parse_args()

    try:
        while True:
            stats = get_queue_stats()
            print_stats(stats)
            time.sleep(args.gather_interval)
    except KeyboardInterrupt:
        print("\x1b[0m")
