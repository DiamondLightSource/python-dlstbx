#
# dlstbx.dc_sim_verify
#   Verify all outstanding simulated data collections against expected
#   results. Create a report as junit.xml file.
#

from __future__ import absolute_import, division, print_function

import datetime
import Queue
import re
import sys
import time
from optparse import SUPPRESS_HELP, OptionParser

import dlstbx.util.result
import ispyb
import junit_xml
import workflows.recipe
from workflows.transport.stomp_transport import StompTransport
import dlstbx.dc_sim.check


processqueue = Queue.Queue()

results_queue = "reduce.dc_sim"
test_results = {}
test_timeout = 3600  # fail scenarios that have not succeeded after 1 hour
forget_test_after = 2 * 24 * 3600  # forget test after 2 days


def process_result(rw, header, message):
    processqueue.put((header, message))


if __name__ == "__main__":
    parser = OptionParser(usage="dlstbx.dc_sim_verify [options]")

    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option(
        "--test",
        action="store_true",
        dest="test",
        help="Run in ActiveMQ testing (zocdev) namespace",
    )
    default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-live.cfg"
    if "--test" in sys.argv:
        default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-testing.cfg"
    # override default stomp host
    StompTransport.load_configuration_file(default_configuration)

    StompTransport.add_command_line_options(parser)
    (options, args) = parser.parse_args(sys.argv[1:])
    stomp = StompTransport()
    stomp.connect()
    txn = stomp.transaction_begin()

    ispyb_conn = ispyb.open("/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg")
    ispyb.model.__future__.enable("/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg")

    sid = workflows.recipe.wrap_subscribe(
        stomp,
        results_queue,
        process_result,
        acknowledgement=True,
        exclusive=True,
        allow_non_recipe_messages=True,
    )

    try:
        while True:
            header, message = processqueue.get(True, 3)
            # Acknowledge all received messages within transaction
            stomp.ack(header, transaction=txn)

            if message.get("summary"):
                # aggregated test information
                for beamline_scenario_string in message["summary"]:
                    test_results.setdefault(beamline_scenario_string, []).extend(
                        message["summary"][beamline_scenario_string]
                    )
                continue
            elif "beamline" in message and "scenario" in message:
                test_results.setdefault(
                    "{m[beamline]}-{m[scenario]}".format(m=message), []
                ).append(message)

    except Queue.Empty:
        pass  # No more messages coming in
    stomp.unsubscribe(sid)
    # Further messages are not processed and will be redelivered to the next instance

    def filter_testruns(runlist):
        """Filter out all historic test runs older then forget_test_after
       and all test runs that are older than the most recent run that
       ended in a result."""
        # Forget everything older than forget_test_after
        forget_all_before = time.time() - forget_test_after
        runlist = [t for t in runlist if t["time_end"] > forget_all_before]

        unfinished = [t for t in runlist if t.get("success") is None]
        finished = [t for t in runlist if t.get("success") is not None]

        # Keep the single most recent test run that has a result...
        if finished:
            latest_outcome = max(t["time_end"] for t in finished)
            finished = [t for t in finished if t["time_end"] == latest_outcome]
            if finished:
                finished = [finished[0]]

        # ...and all test runs that have not yet finished
        return finished + unfinished

    test_results = {
        setting: filter_testruns(testruns) for setting, testruns in test_results.items()
    }

    # Forget tests where all test runs have expired
    for key in list(test_results):
        if not test_results[key]:
            del test_results[key]

    # Check all test runs that do not yet have a definite outcome
    for testruns in test_results.values():
        for testrun in testruns:
            if testrun.get("success") is None:
                print("Verifying", testrun)
                dlstbx.dc_sim.check.check_test_outcome(testrun, ispyb_conn)
                # 3 possible outcomes:
                # The test can be successful (testrun['success'] = True)
                # it can fail (testrun['success'] = False; testrun['reason'] set)
                # or it can be inconclusive (eg. because results are missing)
                # in which case no changes are made

            if (
                testrun.get("success") is None
                and testrun["time_end"] < time.time() - test_timeout
            ):
                print("Rejecting with timeout:", testrun)
                testrun["success"] = False
                testrun["reason"] = "No valid results appeared within timeout"

    # Show all known test results
    from pprint import pprint

    pprint(test_results)

    # If there are results then put summary back on results queue
    if test_results:
        stomp.send(results_queue, {"summary": test_results}, transaction=txn)
    stomp.transaction_commit(txn)

    def synchweb_url(dcid):
        directory = ispyb_conn.get_data_collection(dcid).file_directory
        visit = re.search(r"/([a-z]{2}[0-9]{4,5}-[0-9]+)/", directory)
        if not visit:
            return ""
        visit = visit.group(1)
        return "https://ispyb.diamond.ac.uk/dc/visit/{visit}/id/{dcid}".format(
            visit=visit, dcid=dcid
        )

    # Create JUnit result records
    junit_results = []
    for testruns in test_results.values():
        r = dlstbx.util.result.Result()
        r.set_name(testruns[0]["scenario"])
        r.set_classname("{test[beamline]}.{test[scenario]}".format(test=testruns[0]))
        for test in testruns:
            if test.get("success") in (False, True):
                r.log_message(
                    "Started at {start:%Y-%m-%d %H:%M:%S}, finished at {end:%Y-%m-%d %H:%M:%S}, took {elapsed:.1f} seconds.".format(
                        start=datetime.datetime.fromtimestamp(test["time_start"]),
                        end=datetime.datetime.fromtimestamp(test["time_end"]),
                        elapsed=test["time_end"] - test["time_start"],
                    )
                )
                if test["success"]:
                    r.log_message("Test successful")
                else:
                    r.log_error(test.get("reason", "Test failed"))
                if len(testruns) > 1:
                    r.log_message(
                        "%d further run(s) of this test ongoing" % (len(testruns) - 1)
                    )
                for dcid in test["DCIDs"]:
                    r.log_message(synchweb_url(dcid))
                r.set_time(test["time_end"] - test["time_start"])
                break
        else:
            r.log_message(
                "Started at {start:%Y-%m-%d %H:%M:%S}, finished at {end:%Y-%m-%d %H:%M:%S}, took {elapsed:.1f} seconds.".format(
                    start=datetime.datetime.fromtimestamp(testruns[0]["time_start"]),
                    end=datetime.datetime.fromtimestamp(testruns[0]["time_end"]),
                    elapsed=testruns[0]["time_end"] - testruns[0]["time_start"],
                )
            )
            r.log_skip(
                "Waiting on results, %d instance(s) of this test ongoing"
                % len(testruns)
            )
            for dcid in test["DCIDs"]:
                r.log_message(synchweb_url(dcid))
            r.set_time(testruns[0]["time_end"] - testruns[0]["time_start"])
        junit_results.append(r)

    # Export results
    ts = junit_xml.TestSuite("Simulated data collections", junit_results)
    with open("output.xml", "w") as f:
        junit_xml.TestSuite.to_file(f, [ts], prettyprint=True)

    time.sleep(0.3)
