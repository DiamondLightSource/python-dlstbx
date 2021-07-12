import argparse
import concurrent.futures
import logging

import pkg_resources

import dlstbx.cli.dlq_check
import dlstbx.health_checks as hc
from dlstbx.util.colorstreamhandler import ColorStreamHandler

logger = logging.getLogger("dlstbx.cli.run_health_checks")


def run():
    check_functions = {
        e.name: e.load for e in pkg_resources.iter_entry_points("zocalo.health_checks")
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
        choices=sorted(check_functions),
        help="run specific check only",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=False,
        help="do not write results to database",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        action="store_true",
        default=False,
        help="show more detail",
    )
    options = parser.parse_args()

    if options.graylog:
        dlstbx.enable_graylog(live=True)

    if options.check:
        check_functions = {
            name: loader
            for name, loader in check_functions.items()
            if name in options.check
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

    call_arguments = {
        name: hc.CheckFunctionInterface(
            current_status=current_db_status.copy(), name=name
        )
        for name in check_functions
    }

    deferred_failure = False
    try:
        with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
            results = {
                executor.submit(check_functions[name](), call_arguments[name]): name
                for name in check_functions
            }

            for future in concurrent.futures.as_completed(results):
                name = results[future]
                if not _process_check_result(name, future, db, options):
                    deferred_failure = True
    except KeyboardInterrupt:
        exit(1)
    except BaseException as e:
        logger.critical(f"Encountered unexpected exception: {e}", exc_info=True)
        exit(1)
    if deferred_failure:
        exit(1)


def _process_check_result(name, future, db, options):
    try:
        outcomes = future.result() or []
    except Exception as e:
        logger.error(
            f"Health check {name} raised exception: {e}",
            exc_info=True,
        )
        return True  # might want to handle this internally?
    if isinstance(outcomes, hc.Status):
        outcomes = [outcomes]
    database_error = False
    for outcome in outcomes:
        try:
            if not options.dry_run:
                db.set_status(outcome)
        except Exception as e:
            logger.error(
                f"Could not record {name} outcome {outcome} due to {e}",
                exc_info=True,
            )
            database_error = True
            continue
        verbose_level = f" ({outcome.Level})" if options.verbose else ""
        if outcome.Level >= hc.REPORT.ERROR:
            logger.info(
                f"Recording failure outcome for {outcome.Source} with {outcome.Message}{verbose_level}",
                extra={"level": outcome.Level},
            )
        elif outcome.Level >= hc.REPORT.WARNING:
            logger.info(
                f"Recording warning outcome for {outcome.Source} with {outcome.Message}{verbose_level}",
                extra={"level": outcome.Level},
            )
        else:
            logger.info(
                f"Recording pass outcome for {outcome.Source}{verbose_level}",
                extra={"level": outcome.Level},
            )
    return not database_error
