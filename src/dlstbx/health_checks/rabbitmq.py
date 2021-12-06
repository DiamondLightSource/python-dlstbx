from __future__ import annotations

from datetime import datetime

import zocalo.configuration

import dlstbx
import dlstbx.cli.dlq_check
from dlstbx.cli.get_rabbitmq_statistics import (
    rabbit_checks,
    readable_byte_size,
    readable_time,
)
from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status


def check_rabbitmq_dlq(cfc: CheckFunctionInterface):
    zc = zocalo.configuration.from_file()
    zc.activate_environment("live")
    db_status = cfc.current_status
    status = dlstbx.cli.dlq_check.check_dlq_rabbitmq(zc)
    check_prefix = cfc.name + "."
    now = f"{datetime.now():%Y-%m-%d %H:%M:%S}"

    report_updates = {}
    for queue, messages in status.items():
        if queue.startswith("dlq."):
            queue = queue[4:]
        display_name = queue
        queue = check_prefix + queue

        if messages == 0:
            level = REPORT.PASS
            new_message = f"Error cleared at {now}"
        else:
            level = REPORT.ERROR
            new_message = f"First message seen at {now}"

        if queue in db_status and db_status[queue].MessageBody:
            if level < db_status[queue].Level:
                # error level improved - append message
                new_message = db_status[queue].MessageBody + "\n" + new_message
            elif level == db_status[queue].Level:
                # error level stayed the same - keep message
                new_message = db_status[queue].MessageBody
            # else: error level worsened - replace message

        report_updates[queue] = Status(
            Source=queue,
            Level=level,
            Message=f"{messages} message{'' if messages == 1 else 's'} in {display_name}",
            MessageBody=new_message,
            URL=zc.rabbitmqapi["base_url"],
        )

    for report in db_status:
        if report.startswith(check_prefix):
            if report not in report_updates and db_status[report].Level != REPORT.PASS:
                report_updates[report] = Status(
                    Source=report,
                    Level=REPORT.PASS,
                    Message="Queue removed",
                    MessageBody=db_status[report].MessageBody
                    + "\n"
                    + f"Error cleared at {now}",
                    URL=zc.rabbitmqapi["base_url"],
                )

    return list(report_updates.values())


def check_rabbitmq_health(cfc: CheckFunctionInterface) -> list[Status]:
    check_prefix = cfc.name
    report_updates = {}

    zc = zocalo.configuration.from_file()
    zc.activate_environment("live")
    hosts = ("rabbitmq1", "rabbitmq2", "rabbitmq3")
    system_status = rabbit_checks(zc, [f"{host}.diamond.ac.uk" for host in hosts])
    for host in hosts:
        hs = system_status.get("hosts", {}).get(f"{host}.diamond.ac.uk", {})
        node_status = max(s.level for s in hs.values() if hasattr(s, "level"))
        relevant_status = {
            name: s for name, s in hs.items() if getattr(s, "level", 0) == node_status
        }
        status_array = []
        if not hs:
            node_status = 2
            status_array.append("Could not read node status")
        if "version_rabbitmq" in relevant_status:
            status_array.append(f"RabbitMQ {relevant_status['version_rabbitmq'].text}")
        if "version_erlang" in relevant_status:
            status_array.append(f"Erlang {relevant_status['version_erlang'].text}")
        if "uptime" in relevant_status:
            status_array.append(f"up {readable_time(relevant_status['uptime'].value)}")
        if "memory" in relevant_status:
            status_array.append(
                f"{readable_byte_size(relevant_status['memory'].value)} memory used"
            )
        if "disk" in relevant_status:
            status_array.append(
                f"{readable_byte_size(relevant_status['disk'].value)} disk space"
            )
        if "sockets" in relevant_status:
            status_array.append(f"{relevant_status['sockets'].value:.0f}% sockets used")
        for extra_field in relevant_status.keys() - {
            "memory",
            "disk",
            "sockets",
            "version_rabbitmq",
            "version_erlang",
            "uptime",
        }:
            status_array.append(
                "{field}: {value}".format(
                    field=extra_field,
                    value=getattr(
                        relevant_status[extra_field],
                        "text",
                        getattr(relevant_status[extra_field], "value", None),
                    ),
                )
            )

        if node_status == 0:
            status_object = Status(
                Source=f"{check_prefix}.{host}",
                Level=REPORT.PASS,
                Message="RabbitMQ node running normally",
                MessageBody=", ".join(status_array),
                URL=f"https://{host}.diamond.ac.uk/",
            )
        elif node_status == 1:
            status_object = Status(
                Source=f"{check_prefix}.{host}",
                Level=REPORT.WARNING,
                Message="RabbitMQ node running with warnings",
                MessageBody=", ".join(status_array),
                URL=f"https://{host}.diamond.ac.uk/",
            )
        else:
            status_object = Status(
                Source=f"{check_prefix}.{host}",
                Level=REPORT.ERROR,
                Message="RabbitMQ node failing",
                MessageBody=", ".join(status_array),
                URL=f"https://{host}.diamond.ac.uk/",
            )
        report_updates[f"{check_prefix}.{host}"] = status_object

    if "name" in system_status.get("cluster", {}):
        cluster_status = max(
            s.level for s in system_status["cluster"].values() if hasattr(s, "level")
        )

        def _readable(name):
            if name == "messages_unacknowledged":
                return "in flight"
            return name.capitalize()

        status_array = []
        for name in sorted(system_status["cluster"].keys() - {"name"}):
            value = system_status["cluster"][name]
            status_array.append(
                "{name}: {value}".format(
                    name=_readable(name),
                    value=getattr(value, "value", getattr(value, "text", value)),
                )
            )

        if cluster_status == 0:
            status_object = Status(
                Source=f"{check_prefix}.cluster",
                Level=REPORT.PASS,
                Message=f"RabbitMQ cluster {system_status['cluster']['name'].text} is healthy",
                MessageBody=", ".join(status_array),
                URL=f"https://{hosts[0]}.diamond.ac.uk/",
            )
        elif node_status == 1:
            status_object = Status(
                Source=f"{check_prefix}.cluster",
                Level=REPORT.WARNING,
                Message=f"RabbitMQ cluster {system_status['cluster']['name'].text} is degraded",
                MessageBody=", ".join(status_array),
                URL=f"https://{hosts[0]}.diamond.ac.uk/",
            )
        else:
            status_object = Status(
                Source=f"{check_prefix}.cluster",
                Level=REPORT.ERROR,
                Message=f"RabbitMQ cluster {system_status['cluster']['name'].text} is failing",
                MessageBody=", ".join(status_array),
                URL=f"https://{hosts[0]}.diamond.ac.uk/",
            )
    else:
        status_object = Status(
            Source=f"{check_prefix}.cluster",
            Level=REPORT.ERROR,
            Message="RabbitMQ cluster is down",
            MessageBody="\n".join(
                str(getattr(item, "text", getattr(item, "value", item)))
                for item in system_status.get("cluster", {}).values()
            ),
            URL=f"https://{hosts[0]}.diamond.ac.uk/",
        )
    report_updates[f"{check_prefix}.cluster"] = status_object
    return list(report_updates.values())
