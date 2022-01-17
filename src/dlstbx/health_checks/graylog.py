from __future__ import annotations

import collections
import logging
import re
from operator import itemgetter

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status
from dlstbx.util.certificate import problems_with_certificate
from dlstbx.util.graylog import GraylogAPI

_graylog_host = "graylog2.diamond.ac.uk"
_graylog_url = "https://graylog2.diamond.ac.uk/"
_graylog_dashboard = (
    "https://graylog2.diamond.ac.uk/dashboards/5a5c7f4eddab6253b0d28d1c"
)


def check_graylog_is_alive(cfc: CheckFunctionInterface) -> Status:
    try:
        g = GraylogAPI("/dls_sw/apps/zocalo/secrets/credentials-log.cfg")
        messages = g.get_messages()
    except ConnectionError as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"Connection to Graylog failed with {type(e).__name__}",
            MessageBody=repr(e),
            URL=_graylog_url,
        )

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
            Message="No messages have appeared in Graylog for at least 10 minutes",
            MessageBody="According to Graylog there have not been any messages in the Data Analysis stream for the last 10 minutes",
            URL=_graylog_url,
        )


def check_graylog_is_healthy(cfc: CheckFunctionInterface) -> Status:
    try:
        g = GraylogAPI("/dls_sw/apps/zocalo/secrets/credentials-log.cfg")
        nodes = g.cluster_info()
    except ConnectionError as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"Connection to Graylog failed with {type(e).__name__}",
            MessageBody=repr(e),
            URL=_graylog_url,
        )

    if not nodes:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message="Could not read cluster status",
            MessageBody="The attempt to read the Graylog cluster status failed",
            URL=_graylog_url,
        )

    live_nodes = 0
    issues = []
    for node, node_info in nodes.items():
        if node_info.get("lifecycle") != "running":
            issues.append(f"Node {node} is not running")
            continue
        if node_info.get("lb_status") != "alive":
            issues.append(f"Node {node} is not alive for load-balancing purposes")
            continue
        if node_info.get("is_processing") is not True:
            issues.append(f"Node {node} is not processing incoming messages")
            continue
        live_nodes += 1

    if live_nodes <= 1:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message="Graylog cluster severely degraded",
            MessageBody="\n".join(issues),
            URL=_graylog_url,
        )
    if live_nodes < 3:
        issues.append("Fewer than three nodes present in the cluster")

    certificate_problems = problems_with_certificate(_graylog_host)
    if certificate_problems:
        issues.append(certificate_problems)

    backlog = {node: g.unprocessed_messages(node) for node in sorted(nodes)}
    backlog_sum = 0
    for node, node_info in backlog.items():
        if node_info is False:
            issues.append(f"Could not read backlog information for node {node}")
        else:
            backlog_sum += node_info
            if node_info > 40000:
                issues.append(f"Node {node} has {node_info} messages in backlog")

    if backlog_sum > 200000:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"Large message backlog on Graylog with {backlog_sum} messages",
            MessageBody="\n".join(issues),
            URL=_graylog_url,
        )
    if backlog_sum > 40000:
        return Status(
            Source=cfc.name,
            Level=REPORT.WARNING,
            Message=f"Significant message backlog on Graylog with {backlog_sum} messages",
            MessageBody="\n".join(issues),
            URL=_graylog_url,
        )
    if issues:
        return Status(
            Source=cfc.name,
            Level=REPORT.WARNING,
            Message="Graylog cluster degraded",
            MessageBody="\n".join(issues),
            URL=_graylog_url,
        )
    return Status(
        Source=cfc.name,
        Level=REPORT.PASS,
        Message="Graylog cluster healthy",
        MessageBody=f"Backlog of {backlog_sum} messages",
        URL=_graylog_url,
    )


def check_graylog_has_history(cfc: CheckFunctionInterface) -> Status:
    try:
        g = GraylogAPI("/dls_sw/apps/zocalo/secrets/credentials-log.cfg")
        stats = g.get_history_statistics()
    except ConnectionError as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"Connection to Graylog failed with {type(e).__name__}",
            MessageBody=repr(e),
            URL=_graylog_url,
        )
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
    try:
        g = GraylogAPI("/dls_sw/apps/zocalo/secrets/credentials-log.cfg")
        g.stream = "5d8cd831e7e1f54f98464d3f"  # switch to syslog stream
        g.filters = ["application_name:mmfs", "message:expelling"]

        clusters: collections.Counter = collections.Counter()
        hosts: collections.Counter = collections.Counter()
        errors = 0
        host_and_cluster = re.compile(r"Expelling: [0-9.:]+ \(([^ ]+) in ([^ ]+)\)$")
        for m in g.get_all_messages(time=7200):
            errors += 1
            match = host_and_cluster.search(m["message"])
            if match:
                host, cluster = match.groups()
                hosts[host] += 1
                clusters[cluster] += 1
    except ConnectionError as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"Connection to Graylog failed with {type(e).__name__}",
            MessageBody=repr(e),
            URL=_graylog_url,
        )

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
    try:
        g = GraylogAPI("/dls_sw/apps/zocalo/secrets/credentials-log.cfg")
        g.filters = ["facility:dlstbx.services.filewatcher", "stat-time-max:>5"]

        messages = list(g.get_all_messages(time=1800))
    except ConnectionError as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"Connection to Graylog failed with {type(e).__name__}",
            MessageBody=repr(e),
            URL=_graylog_url,
        )

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
