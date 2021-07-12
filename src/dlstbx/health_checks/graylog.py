import collections
import logging
import re
from operator import itemgetter

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status
from dlstbx.util.graylog import GraylogAPI

_graylog_url = "https://graylog2.diamond.ac.uk/"
_graylog_dashboard = (
    "https://graylog2.diamond.ac.uk/dashboards/5a5c7f4eddab6253b0d28d1c"
)


def check_graylog_is_alive(cfc: CheckFunctionInterface) -> Status:
    g = GraylogAPI("/dls_sw/apps/zocalo/secrets/credentials-log.cfg")
    messages = g.get_messages()

    if messages:
        return Status(
            Source=cfc.name,
            Level=REPORT.PASS,
            Message=f"Messages are appearing in Graylog ({len(messages)} in 10 minutes)",
            URL=_graylog_url,
        )
    else:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"No messages have appeared in Graylog for at least 10 minutes",
            MessageBody="According to Graylog there have not been any messages in the Data Analysis stream for the last 10 minutes",
            URL=_graylog_url,
        )


def check_graylog_has_history(cfc: CheckFunctionInterface) -> Status:
    g = GraylogAPI("/dls_sw/apps/zocalo/secrets/credentials-log.cfg")
    stats = g.get_history_statistics()
    if not stats or not stats.get("range", {}).get("days"):
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message="Could not retrieve Graylog statistics",
            URL=_graylog_dashboard,
        )

    log = logging.getLogger("ithealth.graylog.statistics")
    log.setLevel(logging.DEBUG)
    log.info(
        "graylog statistics update",
        extra={
            "graylog_days": stats["range"]["days"],
            "message_count": stats["message_count"],
        },
    )

    storage_status = (
        f"{stats['range']['days']:.2f} days of message history kept in Graylog"
        f" (total of {stats['message_count']} zocalo messages)"
    )
    if stats["range"]["days"] >= 14:
        result_level = REPORT.PASS
    elif stats["range"]["days"] >= 7:
        result_level = REPORT.WARNING
    else:
        result_level = REPORT.ERROR

    return Status(
        Source=cfc.name,
        Level=result_level,
        Message=storage_status,
        URL=_graylog_dashboard,
    )


def check_gfps_expulsion(cfc: CheckFunctionInterface) -> Status:
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
    return Status(
        Source=cfc.name, Level=level, Message=message, MessageBody=messagebody
    )


def check_filesystem_is_responsive(cfc: CheckFunctionInterface) -> Status:
    g = GraylogAPI("/dls_sw/apps/zocalo/secrets/credentials-log.cfg")
    g.filters = ["facility:dlstbx.services.filewatcher", "stat-time-max:>5"]

    messages = list(g.get_all_messages(time=1800))
    if not messages:
        return Status(
            Source=cfc.name,
            Level=REPORT.PASS,
            Message="Filesystem response times normal",
            MessageBody="No filesystem accesses slower than 5 seconds observed in the last 30 minutes",
            URL=_graylog_dashboard,
        )

    worst_case = sorted(messages, key=itemgetter("stat-time-max"))[-1]
    most_recent = sorted(messages, key=itemgetter("localtime"))[-1]

    if len(messages) == 1 and worst_case["stat-time-max"] <= 15:
        return Status(
            Source=cfc.name,
            Level=REPORT.WARNING,
            Message="Filesystem access time spike detected",
            MessageBody=(
                f"Single slow filesystem stat() call occurred at {worst_case['localtime']:%Y-%m-%d %H:%M:%S},"
                f" taking {worst_case['stat-time-max']:.1f} seconds"
            ),
            URL=_graylog_dashboard,
        )
    return Status(
        Source=cfc.name,
        Level=REPORT.ERROR,
        Message="Filesystems are slower than normal",
        MessageBody=(
            f"{len(messages)} filesystem stat() calls slower than 5 seconds found in the last 30 minutes.\n"
            f"worst occurrence at {worst_case['localtime']:%Y-%m-%d %H:%M:%S} with {worst_case['stat-time-max']:.1f} seconds,\n"
            f"last occurrence at {most_recent['localtime']:%Y-%m-%d %H:%M:%S} with {most_recent['stat-time-max']:.1f} seconds"
        ),
        URL=_graylog_dashboard,
    )
