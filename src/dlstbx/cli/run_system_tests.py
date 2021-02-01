import collections
import logging
import sys
import time

import dlstbx
import dlstbx.system_test
import junit_xml
from dlstbx.util.colorstreamhandler import ColorStreamHandler
from dlstbx.util.result import Result
from workflows.transport.stomp_transport import StompTransport


def run():
    # Set up logging to console and graylog

    logger = logging.getLogger("dlstbx")
    console = ColorStreamHandler()
    # if not debug:
    #  console.setLevel(logging.INFO)
    logger.addHandler(console)
    logger.setLevel(logging.DEBUG)
    logger = logging.getLogger("dlstbx.system_test")

    # Set up transport: override default stomp host
    default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-live.cfg"
    test_mode = False
    if "--test" in sys.argv:
        logger.info("Running on test configuration")
        test_mode = True
        default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-testing.cfg"
        sys.argv = [x for x in sys.argv if x != "--test"]

    # Only log to graylog for live tests
    if not test_mode:
        dlstbx.enable_graylog()

    StompTransport.load_configuration_file(default_configuration)

    transport = StompTransport()
    transport.connect()
    if not transport.is_connected():
        logger.critical("Could not connect to ActiveMQ server")
        sys.exit(1)

    # Load system tests

    dlstbx.system_test.load_all_tests()
    systest_classes = dlstbx.system_test.get_all_tests()
    systest_count = len(systest_classes)
    logger.info("Found %d system test classes" % systest_count)

    if sys.argv[1:] and systest_count:
        systest_classes = {
            n: cls
            for n, cls in systest_classes.items()
            if any(n.lower().startswith(v.lower()) for v in sys.argv[1:])
        }
        logger.info(
            "Filtered %d classes via command line arguments"
            % (systest_count - len(systest_classes))
        )
        systest_count = len(systest_classes)

    tests = {}
    collection_errors = False
    for classname, cls in systest_classes.items():
        logger.debug("Collecting tests from %s" % classname)
        for testname, testsetting in cls(dev_mode=test_mode).collect_tests().items():
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
    logger.info("Found %d system tests" % len(tests))
    if collection_errors:
        sys.exit("Errors during test collection")

    # Set up subscriptions

    print("")

    start_time = time.time()  # This is updated after sending all messages

    channels = collections.defaultdict(list)
    for test, _ in tests.values():
        for expectation in test.expect:
            channels[(expectation["queue"], expectation["topic"])].append(expectation)
        for expectation in test.quiet:
            channels[(expectation["queue"], expectation["topic"])].extend([])

    channel_lookup = {}

    unexpected_messages = Result()
    unexpected_messages.set_name("received_no_unexpected_messages")
    unexpected_messages.set_classname(".")
    unexpected_messages.count = 0

    def handle_receipt(header, message):
        expected_messages = channels[channel_lookup[header["subscription"]]]
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
        unexpected_messages.log_error("Received unexpected message")
        unexpected_messages.log_error(str(header) + "\n" + str(message) + "\n")
        unexpected_messages.count += 1

    for n, (queue, topic) in enumerate(channels.keys()):
        logger.debug("%2d: Subscribing to %s" % (n + 1, queue))
        if queue:
            sub_id = transport.subscribe(queue, handle_receipt)
        if topic:
            sub_id = transport.subscribe_broadcast(topic, handle_receipt)
        channel_lookup[str(sub_id)] = (queue, topic)
        # subscriptions may be expensive on the server side, so apply some rate limiting
        # so that the server can catch up and replies on this connection are not unduly
        # delayed
        time.sleep(0.3)
    logger.debug("Waiting %.1f seconds..." % (0.2 * len(channels)))
    time.sleep(0.2 * len(channels))

    # Send out messages

    print("")

    for test, _ in tests.values():
        for message in test.send:
            if message.get("queue"):
                logger.debug("Sending message to %s", message["queue"])
                transport.send(
                    message["queue"],
                    message["message"],
                    headers=message["headers"],
                    persistent=False,
                )
            if message.get("topic"):
                logger.debug("Broadcasting message to %s", message["topic"])
                transport.broadcast(
                    message["topic"], message["message"], headers=message["headers"]
                )

    # Prepare timer events

    print("")

    start_time = time.time()

    timer_events = []
    for test, _ in tests.values():
        for event in test.timers:
            event["at_time"] = event["at_time"] + start_time
            function = event.get("callback")
            if function:
                args = event.get("args", ())
                kwargs = event.get("kwargs", {})
                x = lambda function=function: function(*args, **kwargs)
            else:
                x = lambda: None
            timer_events.append((event["at_time"], x))
    timer_events = sorted(timer_events, key=lambda tup: tup[0])

    # Wait for messages and timeouts, run events

    keep_waiting = True
    last_message = time.time()
    while keep_waiting:

        # Wait fixed time period or until next event
        wait_to = time.time() + 0.2
        keep_waiting = False
        while timer_events and time.time() > timer_events[0][0]:
            event = timer_events.pop(0)
            event[1]()
        if timer_events:
            wait_to = min(wait_to, timer_events[0][0])
            keep_waiting = True
        if time.time() > last_message + 5:
            logger.info("Waited %5.1fs." % (time.time() - start_time))
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
                            "Test %s.%s timed out waiting for message\n%s"
                            % (testname[0], testname[1], str(expectation))
                        )
                        test[1].log_error("No answer received within time limit.")
                        test[1].log_error(str(expectation))
                    else:
                        keep_waiting = True

    for testname, test in tests.items():
        for expectation in test[0].expect:
            if expectation.get("early"):
                test[1].log_error("Answer received too early.")
                test[1].log_error(str(expectation))
                test[1].early += 1

    # Export results
    ts = junit_xml.TestSuite(
        "dlstbx.system_test", [r for _, r in tests.values()] + [unexpected_messages]
    )
    with open("output.xml", "w") as f:
        junit_xml.TestSuite.to_file(f, [ts], prettyprint=True)

    print("")

    successes = sum(r.is_success() for _, r in tests.values())
    logger.info(
        "System test run completed, %d of %d tests succeeded." % (successes, len(tests))
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
                logger.warning(
                    "  %s %s received %d out of %d expected replies %s"
                    % (
                        b.classname,
                        b.name,
                        len([x for x in a.expect if x.get("received")]),
                        len(a.expect),
                        "(%d early)" % b.early if b.early else "",
                    )
                )
    if unexpected_messages.count:
        logger.error(
            "  Received %d unexpected message%s."
            % (unexpected_messages.count, "" if unexpected_messages.count == 1 else "s")
        )
