import argparse
import collections
import functools
import logging
import re
from datetime import datetime

import dlstbx
import dlstbx.cli.dlq_check
import dlstbx.util.it_health
from dlstbx.util.colorstreamhandler import ColorStreamHandler
from dlstbx.util.graylog import GraylogAPI

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


def check_gfps_expulsion(db_status):
    check = "dls.filesystem.gpfs-expulsion"
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
        level = REPORT_PASS
        message = "No GPFS expulsions seen over the past 2 hours"
        messagebody = ""
    else:
        if errors == 1:
            level = REPORT_WARNING
            message = "One GPFS expulsion seen over the past 2 hours"
        else:
            level = REPORT_ERROR
            message = f"{errors} GPFS expulsions seen over the past 2 hours"
        messagebody = "\n".join(
            ["By cluster group:"]
            + [f"  {count:3d}x {cluster}" for cluster, count in clusters.most_common()]
            + ["", "By host:"]
            + [f"  {count:3d}x {host}" for host, count in clusters.most_common()]
        )
    return Status(Source=check, Level=level, Message=message, MessageBody=messagebody)


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

    check_functions = (check_activemq_dlq, check_gfps_expulsion)

    try:
        for fn in check_functions:
            try:
                outcomes = fn(db_status=current_db_status.copy()) or []
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
