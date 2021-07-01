from datetime import datetime

import dlstbx
import dlstbx.cli.dlq_check
from dlstbx.health_checks import REPORT, CheckFunctionCall, Status


def check_activemq_dlq(cfc: CheckFunctionCall):
    db_status = cfc.current_status
    status = dlstbx.cli.dlq_check.check_dlq()
    check_prefix = "zocalo.dlq.activemq."
    now = f"{datetime.now():%Y-%m-%d %H:%M:%S}"

    report_updates = {}
    for queue, messages in status.items():
        source = queue
        if source.startswith("DLQ."):
            source = source[4:]
        if source.startswith("zocalo."):
            source = source[7:]
        source = check_prefix + source

        if messages == 0:
            level = REPORT.PASS
            new_message = f"Error cleared at {now}"
        else:
            level = REPORT.ERROR
            new_message = f"First message seen at {now}"

        if source in db_status and db_status[source].MessageBody:
            if level < db_status[source].Level:
                # error level improved - append message
                new_message = db_status[source].MessageBody + "\n" + new_message
            elif level == db_status[source].Level:
                # error level stayed the same - keep message
                new_message = db_status[source].MessageBody
            # else: error level worsened - replace message

        report_updates[source] = Status(
            Source=source,
            Level=level,
            Message=f"{messages} message{'' if messages == 1 else 's'} in {queue}",
            MessageBody=new_message,
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
                )

    return report_updates.values()
