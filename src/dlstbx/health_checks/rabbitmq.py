from datetime import datetime

import zocalo.configuration

import dlstbx
import dlstbx.cli.dlq_check
from dlstbx.cli.get_rabbitmq_statistics import RabbitMQAPI
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


def check_rabbitmq_health(cfc: CheckFunctionInterface):
    zc = zocalo.configuration.from_file()
    zc.activate_environment("live")
    rmq = RabbitMQAPI(zc)

    db_status = cfc.current_status
    check_prefix = cfc.name
    report_updates = {}
    now = f"{datetime.now():%Y-%m-%d %H:%M:%S}"

    success, failures = rmq.health_checks()
    for check, msg in failures.items():
        report_updates[check] = Status(
            Source=check_prefix + check.replace("/", "."),
            Level=REPORT.ERROR,
            Message="RabbitMQ is running outside normal parameters",
            MessageBody=msg,
            URL=zc.rabbitmqapi["base_url"],
        )

    for node in rmq.nodes:
        for alarm in {"disk_free_alarm", "mem_alarm"}:
            check = check_prefix + "." + alarm
            if node[alarm]:
                report_updates[check] = Status(
                    Source=check,
                    Level=REPORT.ERROR,
                    Message="RabbitMQ is running outside normal parameters",
                    MessageBody=f"{node['name']}: {alarm}={node[alarm]}",
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

    return list(report_updates.values())
