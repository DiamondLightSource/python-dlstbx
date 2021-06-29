import argparse
import functools
import logging
from datetime import datetime

import dlstbx
import dlstbx.cli.dlq_check
import dlstbx.util.it_health
from dlstbx.util.colorstreamhandler import ColorStreamHandler

REPORT_ERROR = 20
REPORT_WARNING = 10
REPORT_PASS = 0

Status = functools.partial(
    dlstbx.util.it_health.Status, Level=REPORT_PASS, Timestamp=datetime.now()
)

logger = logging.getLogger("dlstbx.cli.run_health_checks")


def check_activemq_dlq(db_status):
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
            level = REPORT_PASS
            new_message = f"Error cleared at {now}"
        else:
            level = REPORT_ERROR
            new_message = f"First message seen at {now}"

        if source in db_status and db_status[source].MessageBody:
            if level < db_status[source].Level:
                # error level improved - append message
                new_message = db_status[source].MessageBody + "\n" + new_message
            elif level == db_status[source].Level:
                # error level stays the same - keep message
                new_message = db_status[source].MessageBody
            # else: error level worsened - replace message

        report_updates[source] = Status(
            Source=source,
            Level=level,
            Message=f"{messages} message{'s' if messages > 1 else ''} in {queue}",
            MessageBody=new_message,
        )

    for report in db_status:
        if report.startswith(check_prefix):
            if report not in report_updates and db_status[report].Level != REPORT_PASS:
                report_updates[report] = Status(
                    Source=report,
                    Level=REPORT_PASS,
                    Message="Queue removed",
                    MessageBody=db_status[report].MessageBody
                    + "\n"
                    + f"Error cleared at {now}",
                )

    return report_updates.values()


def run():
    parser = argparse.ArgumentParser(
        description="Run infrastructure checks and report results to a database."
    )

    parser.add_argument("-?", action="help", help=argparse.SUPPRESS)
    parser.add_argument(
        "--graylog",
        dest="graylog",
        action="store_true",
        default=False,
        help="enable logging to graylog",
    )
    options = parser.parse_args()

    if options.graylog:
        dlstbx.enable_graylog(live=True)

    root_logger = logging.getLogger("dlstbx")
    root_logger.setLevel(logging.INFO)
    console = ColorStreamHandler()
    console.setLevel(logging.DEBUG if True else logging.INFO)
    logging.getLogger().addHandler(console)
    logging.getLogger("ithealth").setLevel(logging.CRITICAL)

    try:
        db = dlstbx.util.it_health.database()
        current_db_status = {s.Source: s for s in db.get_status()}
    except Exception as e:
        logger.error(f"Could not connect to IT health database: {e}", exc_info=True)
        exit(1)

    check_functions = (check_activemq_dlq,)

    try:
        for fn in check_functions:
            try:
                outcomes = fn(db_status=current_db_status.copy())
            except Exception as e:
                logger.error(
                    f"Health check function {fn.__name__} raised exception: {e}",
                    exc_info=True,
                )
                continue
            if isinstance(outcomes, dlstbx.util.it_health.Status):
                outcomes = [outcomes]
            for outcome in outcomes:
                try:
                    db.set_status(outcome)
                except Exception as e:
                    logger.error(
                        f"Could not record {fn.__name__} outcome {outcome} due to raised exception: {e}",
                        exc_info=True,
                    )
                    continue
                if outcome.Level >= REPORT_ERROR:
                    logger.info(
                        f"Recording outcome for {fn.__name__}: {outcome.Source} failed with {outcome.Message}"
                    )
                elif outcome.Level >= REPORT_WARNING:
                    logger.info(
                        f"Recording outcome for {fn.__name__}: {outcome.Source} warned with {outcome.Message}"
                    )
                else:
                    logger.info(
                        f"Recording outcome for {fn.__name__}: {outcome.Source} passed"
                    )
    except KeyboardInterrupt:
        exit(1)
    except BaseException as e:
        logger.critical(f"Encountered unexpected exception: {e}", exc_info=True)
        exit(1)
