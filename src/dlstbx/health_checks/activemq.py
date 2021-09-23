from datetime import datetime

import zocalo.configuration

import dlstbx
import dlstbx.cli.dlq_check
from dlstbx.cli.get_activemq_statistics import ActiveMQAPI
from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status


def check_activemq_dlq(cfc: CheckFunctionInterface):
    zc = zocalo.configuration.from_file()
    zc.activate_environment("live")
    db_status = cfc.current_status
    status = dlstbx.cli.dlq_check.check_dlq(zc)
    check_prefix = cfc.name + "."
    now = f"{datetime.now():%Y-%m-%d %H:%M:%S}"

    report_updates = {}
    for queue, messages in status.items():
        if queue.startswith("DLQ."):
            queue = queue[4:]
        display_name = queue
        if queue.startswith("zocalo."):
            queue = queue[7:]
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
            URL="http://activemq.diamond.ac.uk/",
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
                    URL="http://activemq.diamond.ac.uk/",
                )

    return list(report_updates.values())


def _format_number(n):
    if n > 3000000000:
        return f"{n/1000000000:.1f}G"
    elif n > 3000000:
        return f"{n/1000000:.1f}M"
    elif n > 3000:
        return f"{n/1000:.1f}K"
    else:
        return n


def check_activemq_health(cfc: CheckFunctionInterface):
    db_status = cfc.current_status
    check_prefix = cfc.name + "."

    GB = 1024 * 1024 * 1024
    checks = {
        check_prefix + "storage.persistent": ("StorePercentUsage", 25, 50),
        check_prefix + "storage.temporary": ("TempPercentUsage", 25, 50),
        check_prefix + "storage.memory": ("MemoryPercentUsage", 50, 75),
        check_prefix + "connections": ("ConnectionsCount", 650, 850),
        check_prefix + "heap_memory": ("HeapMemoryUsed", 55 * GB, 59 * GB),
    }
    report_updates = {}
    now = f"{datetime.now():%Y-%m-%d %H:%M:%S}"

    amq = ActiveMQAPI()
    amq.connect()
    available_keys = {k[3:].lower(): k for k in dir(amq) if k.startswith("get")}
    for check in checks:
        check_key, check_warning, check_limit = checks[check]
        check_function = getattr(amq, available_keys[check_key.lower()])
        value = check_function()
        if value is None:
            report_updates[check] = Status(
                Source=check,
                Level=REPORT.ERROR,
                Message="ActiveMQ is running outside normal parameters",
                MessageBody=f"Could not determine value for {check_key}",
                URL="http://activemq.diamond.ac.uk/",
            )
        elif value > check_limit:
            report_updates[check] = Status(
                Source=check,
                Level=REPORT.ERROR,
                Message="ActiveMQ is running outside normal parameters",
                MessageBody=f"{check_key}: {_format_number(value)}, which exceeds error threshold of {_format_number(check_limit)}",
                URL="http://activemq.diamond.ac.uk/",
            )
        elif value > check_warning:
            report_updates[check] = Status(
                Source=check,
                Level=REPORT.WARNING,
                Message="ActiveMQ is running outside normal parameters",
                MessageBody=f"{check_key}: {_format_number(value)}, which exceeds warning threshold of {_format_number(check_warning)}",
                URL="http://activemq.diamond.ac.uk/",
            )

    for report in db_status:
        for check in checks:
            if (
                check in db_status
                and check not in report_updates
                and db_status[check].Level != REPORT.PASS
            ):
                report_updates[check] = Status(
                    Source=check,
                    Level=REPORT.PASS,
                    Message="ActiveMQ is running normally",
                    MessageBody=(db_status[report].MessageBody or "")
                    + "\n"
                    + f"Error cleared at {now}",
                    URL="http://activemq.diamond.ac.uk/",
                )

    return list(report_updates.values())
