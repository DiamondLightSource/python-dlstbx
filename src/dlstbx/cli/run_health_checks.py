import argparse
import logging
from datetime import datetime

import dlstbx.cli.dlq_check
import dlstbx.health_checks as hc
import dlstbx.health_checks.activemq
import dlstbx.health_checks.graylog
from dlstbx.util.colorstreamhandler import ColorStreamHandler

logger = logging.getLogger("dlstbx.cli.run_health_checks")


def run():
    check_functions = {
        "activemq": dlstbx.health_checks.activemq.check_activemq_dlq,
        "gpfs": dlstbx.health_checks.graylog.check_gfps_expulsion,
        "graylog": dlstbx.health_checks.graylog.check_graylog_is_alive,
        "slowfs": dlstbx.health_checks.graylog.check_filesystem_is_responsive,
    }

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
    parser.add_argument(
        "--check",
        dest="check",
        action="append",
        metavar="CHK",
        default=[],
        type=str,
        choices=tuple(check_functions),
        help="run specific check only",
    )
    options = parser.parse_args()

    if options.graylog:
        dlstbx.enable_graylog(live=True)

    if options.check:
        check_functions = {
            key: value for key, value in check_functions.items() if key in options.check
        }

    root_logger = logging.getLogger("dlstbx")
    root_logger.setLevel(logging.INFO)
    console = ColorStreamHandler()
    console.setLevel(logging.DEBUG if True else logging.INFO)
    logging.getLogger().addHandler(console)
    logging.getLogger("ithealth").setLevel(logging.CRITICAL)

    try:
        db = hc.database()
        current_db_status = {s.Source: s for s in db.get_status()}
    except Exception as e:
        logger.error(f"Could not connect to IT health database: {e}", exc_info=True)
        exit(1)

    try:
        hc.timestamp_default = datetime.now()
        for fn in check_functions.values():
            call_args = hc.CheckFunctionCall(current_status=current_db_status.copy())
            try:
                outcomes = fn(call_args) or []
            except Exception as e:
                logger.error(
                    f"Health check function {fn.__name__} raised exception: {e}",
                    exc_info=True,
                )
                continue
            if isinstance(outcomes, hc.Status):
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
                if outcome.Level >= hc.REPORT.ERROR:
                    logger.info(
                        f"Recording outcome for {fn.__name__}: {outcome.Source} failed with {outcome.Message}"
                    )
                elif outcome.Level >= hc.REPORT.WARNING:
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
