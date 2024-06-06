from __future__ import annotations

import argparse
import collections
import logging
import operator
import sys
import time

import junit_xml
import workflows.transport
import zocalo.configuration

import dlstbx
import dlstbx.system_test
from dlstbx.util.colorstreamhandler import ColorStreamHandler
from dlstbx.util.result import Result

TimerEvent = collections.namedtuple(
    "TimerEvent", "time, callback, expected_result, result_object"
)


def run():
    # Set up logging to console and graylog

    logger = logging.getLogger("dlstbx")
    console = ColorStreamHandler()
    # if not debug:
    #  console.setLevel(logging.INFO)
    logger.addHandler(console)
    logger.setLevel(logging.DEBUG)
    logger = logging.getLogger("dlstbx.system_test")

    # Load system tests
    dlstbx.system_test.load_all_tests()
    systest_classes = dlstbx.system_test.get_all_tests()

    parser = argparse.ArgumentParser(
        usage="dlstbx.run_system_tests [options]",
        description="Run Zocalo system tests",
    )
    parser.add_argument(
        "tests",
        nargs="*",
        help="You can specify one or multiple individual tests to run, "
        "or not specify any and therefore run all. Available tests: "
        + ", ".join(sorted(systest_classes)),
    )
    parser.add_argument("-?", action="help", help=argparse.SUPPRESS)
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        metavar="TEST",
        choices=sorted(systest_classes),
        type=str,
        help="Exclude one of the above named tests. Spelling must match exactly",
    )
    parser.add_argument(
        "--output",
        action="store_true",
        help="Write an output.xml junit XML file summary",
    )

    # Load configuration
    zc = zocalo.configuration.from_file()
    zc.activate()
    zc.add_command_line_options(parser)
    workflows.transport.add_command_line_options(parser, transport_argument=True)

    args = parser.parse_args()
    test_mode = {"devrmq", "devamq"} & set(zc.active_environments)

    transport = workflows.transport.lookup(args.transport)()
    transport.connect()
    if not transport.is_connected():
        logger.critical("Could not connect to message broker")
        sys.exit(1)

    systest_count = len(systest_classes)
    logger.info(f"Found {systest_count} system test classes")

    if args.tests and systest_count:
        systest_classes = {
            n: cls
            for n, cls in systest_classes.items()
            if any(n.lower().startswith(v.lower()) for v in args.tests)
        }
        logger.info(
            f"Filtered {systest_count - len(systest_classes)} classes via command line arguments"
        )
        systest_count = len(systest_classes)
    if args.exclude and systest_count:
        systest_classes = {
            n: cls for n, cls in systest_classes.items() if n not in args.exclude
        }
        logger.info(
            f"Excluded {systest_count - len(systest_classes)} classes via command line arguments"
        )
        systest_count = len(systest_classes)

    def handle_receipt(header, message):
        for expected_message in expected_messages:
            if not expected_message.get("received"):
                if expected_message["message"] == message:
                    if expected_message.get("headers"):
                        headers_match = True
                        for parameter, value in expected_message["headers"].items():
                            if value != header.get(parameter):
                                headers_match = False
                        if not headers_match:
                            logger.warning(
                                "Received a message similar to an expected message:\n"
                                + str(message)
                                + "\n but its header\n"
                                + str(header)
                                + "\ndoes not match the expected header:\n"
                                + str(expected_message["headers"])
                            )
                            continue
                    if (
                        expected_message.get("min_wait")
                        and (time.time() - start_time) < expected_message["min_wait"]
                    ):
                        expected_message["early"] = (
                            "Received expected message:\n"
                            + str(header)
                            + "\n"
                            + str(message)
                            + "\n%.1f seconds too early."
                            % (expected_message["min_wait"] + start_time - time.time())
                        )
                        logger.warning(expected_message["early"])
                    expected_message["received"] = True
                    logger.debug(
                        "Received expected message:\n"
                        + str(header)
                        + "\n"
                        + str(message)
                        + "\n"
                    )
                    return
        logger.warning(
            "Received unexpected message:\n"
            + str(header)
            + "\n"
            + str(message)
            + "\n which is not in \n"
            + str(expected_messages)
            + "\n"
        )
        unexpected_messages.log_error(
            message="Received unexpected message",
            output=str(header) + "\n" + str(message) + "\n",
        )
        unexpected_messages.count += 1

    queue_subscription = transport.subscribe_temporary("system_tests", handle_receipt)
    logger.debug(f"{queue_subscription=}")

    tests = {}
    collection_errors = False
    for classname, cls in systest_classes.items():
        logger.debug(f"Collecting tests from {classname}")
        for testname, testsetting in (
            cls(zc=zc, dev_mode=test_mode, target_queue=queue_subscription.queue_name)
            .collect_tests()
            .items()
        ):
            testresult = Result()
            testresult.set_name(testname)
            testresult.set_classname(classname)
            testresult.early = 0
            if testsetting.errors:
                testresult.log_trace("\n".join(testsetting.errors))
                logger.error(
                    "Error reading test %s:\n%s",
                    testname,
                    "\n".join(testsetting.errors),
                )
                collection_errors = True
            tests[(classname, testname)] = (testsetting, testresult)
    logger.info(f"Found {len(tests)} system tests")
    if collection_errors:
        sys.exit("Errors during test collection")

    # Set up subscriptions

    print("")

    start_time = time.time()  # This is updated after sending all messages

    expected_messages = [
        expectation for test, _ in tests.values() for expectation in test.expect
    ]
    logger.debug(f"Expected messages: {expected_messages}")

    unexpected_messages = Result()
    unexpected_messages.set_name("received_no_unexpected_messages")
    unexpected_messages.set_classname(".")
    unexpected_messages.count = 0

    # Send out messages

    print("")

    for test, _ in tests.values():
        for message in test.send:
            if message.get("queue"):
                logger.debug(f"Sending message to {message['queue']}")
                transport.send(
                    message["queue"],
                    message["message"],
                    headers=message["headers"],
                    persistent=False,
                )
            if message.get("topic"):
                logger.debug(f"Broadcasting message to {message['topic']}")
                transport.broadcast(
                    message["topic"], message["message"], headers=message["headers"]
                )

    # Prepare timer events

    print("")

    start_time = time.time()

    timer_events = []
    for test, result in tests.values():
        for event in test.timers:
            event["at_time"] = event["at_time"] + start_time
            function = event.get("callback")
            if function:
                fargs = event.get("args", ())
                fkwargs = event.get("kwargs", {})
                timer_events.append(
                    TimerEvent(
                        time=event["at_time"],
                        result_object=result,
                        callback=lambda function=function: function(*fargs, **fkwargs),
                        expected_result=event.get("expect_return", Ellipsis),
                    )
                )
            else:
                timer_events.append(
                    TimerEvent(
                        time=event["at_time"],
                        result_object=result,
                        callback=lambda: None,
                        expected_result=Ellipsis,
                    )
                )
    timer_events = sorted(timer_events, key=operator.attrgetter("time"))

    # Wait for messages and timeouts, run events

    keep_waiting = True
    last_message = time.time()
    while keep_waiting:
        # Wait fixed time period or until next event
        wait_to = time.time() + 0.2
        keep_waiting = False
        while timer_events and time.time() > timer_events[0].time:
            event = timer_events.pop(0)
            event_result = event.callback()
            if event.expected_result is not Ellipsis:
                if event.expected_result != event_result:
                    logger.warning(
                        f"{event.result_object.classname} timer event failed for {event.result_object.name}: return value '{event_result}' does not match '{event.expected_result}'"
                    )
                    event.result_object.log_error(
                        message=f"Timer event failed with result {event_result} instead of expected {event.expected_result}"
                    )
        if timer_events:
            wait_to = min(wait_to, timer_events[0][0])
            keep_waiting = True
        if time.time() > last_message + 5:
            wait_time = time.time() - start_time
            logger.info(f"Waited {wait_time:5.1f}s.")
            last_message = time.time()
        time.sleep(max(0.01, wait_to - time.time()))

        for testname, test in tests.items():
            for expectation in test[0].expect:
                if not expectation.get("received") and not expectation.get(
                    "received_timeout"
                ):
                    if time.time() > start_time + expectation["timeout"]:
                        expectation["received_timeout"] = True
                        logger.warning(
                            f"Test {testname[0]}.{testname[1]} timed out waiting for message\n{expectation}"
                        )
                        test[1].log_error(
                            message="No answer received within time limit.",
                            output=str(expectation),
                        )
                    else:
                        keep_waiting = True

    for testname, test in tests.items():
        for expectation in test[0].expect:
            if expectation.get("early"):
                test[1].log_error(
                    message="Answer received too early.", output=str(expectation)
                )
                test[1].early += 1

    if args.output:
        # Export results
        ts = junit_xml.TestSuite(
            "dlstbx.system_test", [r for _, r in tests.values()] + [unexpected_messages]
        )
        with open("output.xml", "w") as f:
            junit_xml.TestSuite.to_file(f, [ts], prettyprint=True)

    print("")

    successes = sum(r.is_success() for _, r in tests.values())
    logger.info(
        f"System test run completed, {successes} of {len(tests)} tests succeeded."
    )
    for a, b in tests.values():
        if not b.is_success():
            if b.is_failure() and b.failure_output:
                logger.error(
                    "  %s %s failed:\n    %s",
                    b.classname,
                    b.name,
                    b.failure_output.replace("\n", "\n    "),
                )
            else:
                received_count = len([x for x in a.expect if x.get("received")])
                expected_count = len(a.expect)
                logger.warning(
                    (
                        f"  {b.classname} {b.name} received {received_count} out of {expected_count} expected replies"
                        + f" ({b.early} early)"
                        if b.early
                        else ""
                    ),
                )
    if unexpected_messages.count:
        logger.error(
            f"  Received {unexpected_messages.count} unexpected message{'' if unexpected_messages.count == 1 else 's'}."
        )
        exit(1)
    if successes != len(tests):
        exit(1)
