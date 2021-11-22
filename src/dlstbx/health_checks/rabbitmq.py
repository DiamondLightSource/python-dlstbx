from __future__ import annotations

from datetime import datetime

import zocalo.configuration
from zocalo.util.rabbitmq import RabbitMQAPI

import dlstbx
import dlstbx.cli.dlq_check
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
    zc = zocalo.configuration.from_file()
    zc.activate_environment("live")
    hosts = ["rabbitmq1", "rabbitmq2", "rabbitmq3"]
    api = {
        host: RabbitMQAPI(
            url=f"https://{host}.diamond.ac.uk/api",
            user=zc.rabbitmqapi["username"],
            password=zc.rabbitmqapi["password"],
        )
        for host in hosts
    }

    db_status = cfc.current_status
    check_prefix = cfc.name
    report_updates = {}
    now = f"{datetime.now():%Y-%m-%d %H:%M:%S}"

    # 1. is every host up?
    # 2. does every host think the entire cluster is up?
    # 3. now check more stuff

    nodes_seen_by = {}
    for host in hosts:
        try:
            nodes_seen_by[host] = api[host].nodes()
            from pprint import pprint

            pprint(api[host].get("overview"))
            exit(1)
            pprint(nodes_seen_by[host])
            report_updates[f"{check_prefix}.{host}.status"] = Status(
                Source=f"{check_prefix}.{host}.status",
                Level=REPORT.PASS,
                Message=f"Node {host} is running",
                URL=f"https://{host}.diamond.ac.uk/",
            )
        except Exception as e:
            report_updates[f"{check_prefix}.{host}.status"] = Status(
                Source=f"{check_prefix}.{host}.status",
                Level=REPORT.ERROR,
                Message=f"Node status API call on {host} resulted in {type(e).__name__}",
                MessageBody=str(e),
                URL=f"https://{host}.diamond.ac.uk/",
            )
    if not nodes_seen_by:
        report_updates[check_prefix] = Status(
            Source=check_prefix,
            Level=REPORT.ERROR,
            Message="RabbitMQ cluster is down",
        )
        return list(report_updates.values())

    if (
        len(
            {
                frozenset(nodeinfo.name for nodeinfo in seen_nodes)
                for seen_nodes in nodes_seen_by.values()
            }
        )
        != 1
    ):
        report_updates[check_prefix] = Status(
            Source=check_prefix,
            Level=REPORT.ERROR,
            Message="RabbitMQ cluster is running with a net split",
            MessageBody="\n".join(
                f"{host} sees nodes: {', '.join(nodeinfo.name for nodeinfo in nodes_seen_by[host])}"
                for host in sorted(nodes_seen_by)
            ),
        )
    elif any(len(seen) < 2 for seen in nodes_seen_by.values()):
        report_updates[check_prefix] = Status(
            Source=check_prefix,
            Level=REPORT.ERROR,
            Message="RabbitMQ cluster is degraded",
        )
    elif len(nodes_seen_by) != len(hosts) or any(
        len(seen) != len(hosts) for seen in nodes_seen_by.values()
    ):
        report_updates[check_prefix] = Status(
            Source=check_prefix,
            Level=REPORT.WARNING,
            Message="RabbitMQ cluster is degraded",
        )
    else:
        report_updates[check_prefix] = Status(
            Source=check_prefix,
            Level=REPORT.PASS,
            Message="RabbitMQ cluster is healthy",
        )

    return list(report_updates.values())

    success, failures = rmq.health_checks
    for check, msg in failures.items():
        report_updates[check] = Status(
            Source=check_prefix + check.replace("/", "."),
            Level=REPORT.ERROR,
            Message="RabbitMQ is running outside normal parameters",
            MessageBody=msg,
            URL=zc.rabbitmqapi["base_url"],
        )
    from pprint import pprint

    pprint(rmq.nodes())
    pprint(success)
    pprint(failures)

    for node in rmq.nodes():
        for alarm in {"disk_free_alarm", "mem_alarm"}:
            check = check_prefix + "." + alarm
            if getattr(node, alarm):
                report_updates[check] = Status(
                    Source=check,
                    Level=REPORT.ERROR,
                    Message="RabbitMQ is running outside normal parameters",
                    MessageBody=f"{node.name}: {alarm}={node.alarm}",
                    URL=zc.rabbitmqapi["base_url"],
                )
            elif check in db_status and db_status[check].Level != REPORT.PASS:
                report_updates[check] = Status(
                    Source=check,
                    Level=REPORT.PASS,
                    MessageBody=(db_status[check].MessageBody or "")
                    + "\n"
                    + f"Error cleared at {now}",
                    URL=zc.rabbitmqapi["base_url"],
                )

    for report in db_status:
        for check in success:
            if (
                check in db_status
                and check not in report_updates
                and db_status[check].Level != REPORT.PASS
            ):
                report_updates[check] = Status(
                    Source=check_prefix + check.replace("/", "."),
                    Level=REPORT.PASS,
                    Message="RabbitMQ is running normally",
                    MessageBody=(db_status[report].MessageBody or "")
                    + "\n"
                    + f"Error cleared at {now}",
                    URL=zc.rabbitmqapi["base_url"],
                )

    pprint(report_updates)
    return list(report_updates.values())
