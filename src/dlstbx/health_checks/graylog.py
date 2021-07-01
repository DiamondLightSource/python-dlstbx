import collections
import re

from dlstbx.health_checks import REPORT, CheckFunctionCall, Status
from dlstbx.util.graylog import GraylogAPI


def check_graylog_is_alive(cfc: CheckFunctionCall) -> Status:
    check = "services.graylog.alive"
    g = GraylogAPI("/dls_sw/apps/zocalo/secrets/credentials-log.cfg")
    messages = g.get_messages()

    if messages:
        return Status(
            Source=check,
            Level=REPORT.PASS,
            Message=f"Messages are appearing in Graylog ({len(messages)} in 10 minutes)",
            URL="https://graylog2.diamond.ac.uk/",
        )
    else:
        return Status(
            Source=check,
            Level=REPORT.ERROR,
            Message=f"No messages have appeared in Graylog for at least 10 minutes",
            MessageBody="According to Graylog there have not been any messages in the Data Analysis stream for the last 10 minutes",
            URL="https://graylog2.diamond.ac.uk/",
        )


def check_gfps_expulsion(cfc: CheckFunctionCall) -> Status:
    check = "it.filesystem.gpfs-expulsion"
    g = GraylogAPI("/dls_sw/apps/zocalo/secrets/credentials-log.cfg")
    g.stream = "5d8cd831e7e1f54f98464d3f"  # switch to syslog stream
    g.filters = ["application_name:mmfs", "message:expelling"]

    errors, hosts, clusters = 0, collections.Counter(), collections.Counter()
    host_and_cluster = re.compile(r"Expelling: [0-9.:]+ \(([^ ]+) in ([^ ]+)\)$")
    for m in g.get_all_messages(time=7200):
        errors += 1
        match = host_and_cluster.search(m["message"])
        if match:
            host, cluster = match.groups()
            hosts[host] += 1
            clusters[cluster] += 1

    if errors == 0:
        level = REPORT.PASS
        message = "No nodes ejected from GPFS in the past 2 hours"
        messagebody = ""
    else:
        if errors == 1:
            level = REPORT.WARNING
            message = "One node ejection from GPFS seen in the past 2 hours"
        else:
            level = REPORT.ERROR
            message = f"{errors} node ejections from GPFS seen in the past 2 hours"
        messagebody = "\n".join(
            ["By cluster group:"]
            + [f"  {count:3d}x {cluster}" for cluster, count in clusters.most_common()]
            + ["", "By host:"]
            + [f"  {count:3d}x {host}" for host, count in clusters.most_common()]
        )
    return Status(Source=check, Level=level, Message=message, MessageBody=messagebody)
