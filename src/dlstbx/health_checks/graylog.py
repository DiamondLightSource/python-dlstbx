import collections
import re
from operator import itemgetter

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status
from dlstbx.util.graylog import GraylogAPI


def check_graylog_is_alive(cfc: CheckFunctionInterface) -> Status:
    check = "services.graylog.alive"
    url = "https://graylog2.diamond.ac.uk/"
    g = GraylogAPI("/dls_sw/apps/zocalo/secrets/credentials-log.cfg")
    messages = g.get_messages()

    if messages:
        return Status(
            Source=check,
            Level=REPORT.PASS,
            Message=f"Messages are appearing in Graylog ({len(messages)} in 10 minutes)",
            URL=url,
        )
    else:
        return Status(
            Source=check,
            Level=REPORT.ERROR,
            Message=f"No messages have appeared in Graylog for at least 10 minutes",
            MessageBody="According to Graylog there have not been any messages in the Data Analysis stream for the last 10 minutes",
            URL=url,
        )


def check_gfps_expulsion(cfc: CheckFunctionInterface) -> Status:
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


def check_filesystem_is_responsive(cfc: CheckFunctionInterface) -> Status:
    check = "it.filesystem.responsiveness"
    url = "https://graylog2.diamond.ac.uk/dashboards/5a5c7f4eddab6253b0d28d1c"
    g = GraylogAPI("/dls_sw/apps/zocalo/secrets/credentials-log.cfg")
    g.filters = ["facility:dlstbx.services.filewatcher", "stat-time-max:>5"]

    messages = list(g.get_all_messages(time=1800))
    if not messages:
        return Status(
            Source=check,
            Level=REPORT.PASS,
            Message="Filesystem response times normal",
            MessageBody="No filesystem accesses slower than 5 seconds observed in the last 30 minutes",
            URL=url,
        )

    worst_case = sorted(messages, key=itemgetter("stat-time-max"))[-1]
    most_recent = sorted(messages, key=itemgetter("localtime"))[-1]
    return Status(
        Source=check,
        Level=REPORT.ERROR,
        Message="Filesystems are slower than normal",
        MessageBody=(
            f"{len(messages)} filesystem stat() calls slower than 5 seconds found in the last 30 minutes.\n"
            f"worst occurrence at {worst_case['localtime']:%Y-%m-%d %H:%M:%S} with {worst_case['stat-time-max']:.1f} seconds,\n"
            f"last occurrence at {most_recent['localtime']:%Y-%m-%d %H:%M:%S} with {most_recent['stat-time-max']:.1f} seconds"
        ),
        URL=url,
    )
