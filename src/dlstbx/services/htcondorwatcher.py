from __future__ import annotations

import time
from pprint import pformat

import workflows.recipe
from workflows.services.common_service import CommonService

from dlstbx.util.profiler import Profiler


class HTCondorWatcher(CommonService):
    """
    A service that waits for HTCondor jobs to complete and notifies interested
    parties when they do, or don't.
    """

    # Human readable service name
    _service_name = "HTCondorwatcher"

    # Logger name
    _logger_name = "dlstbx.services.htcondorwatcher"

    def initializing(self):
        """
        Subscribe to the htcondorwatcher queue. Received messages must be
        acknowledged.
        """
        self.log.info("HTCondorwatcher starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "htcondorwatcher",
            self.watch_jobs,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def watch_jobs(self, rw, header, message):
        """
        Watch for a given list of jobs.
        """
        # Check if message body contains partial results from a previous run
        start_time = time.time()
        if isinstance(message, dict):
            first_seen = message.get("first-seen", start_time)

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        # Keep a record of os.stat timings
        os_stat_profiler = Profiler()

        # Look for jobs
        import htcondor

        coll = htcondor.Collector(htcondor.param["COLLECTOR_HOST"])
        schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
        schedd = htcondor.Schedd(schedd_ad)

        # List jobs to wait for
        try:
            joblist = rw.payload["jobid"]
            if isinstance(joblist, int):
                joblist = [joblist]
            jobcount = len(joblist)
        except KeyError:
            self.log.error("Field 'jobid' is missing from the received message.")
            rw.transport.transaction_commit(txn)
            return
        except TypeError:
            self.log.error(f"Invalid received message type: {type(rw.payload)}.")
            rw.transport.transaction_commit(txn)
            return

        # If the only entry in the list is 'None' then there are no jobs to
        # watch for. Bail out early and only notify on 'finally'.
        if jobcount == 1 and joblist[0] is None:
            self.log.debug("Empty list encountered")
            rw.send_to(
                "finally",
                {"jobs-expected": 0, "jobs-seen": 0, "success": True},
                transaction=txn,
            )
            rw.transport.transaction_commit(txn)
            return

        seen_jobs = []
        for jobid in joblist:
            # while (
            #    status["seen-jobs"] < jobcount
            #    and jobs_found < rw.recipe_step["parameters"].get("burst-limit", 100)
            #    and joblist[status["seen-jobs"]]
            # ):
            with os_stat_profiler.record():
                res = schedd.query(
                    constraint=f"ClusterId=={jobid}",
                    projection=["ClusterId", "ProcId", "JobStatus", "Out"],
                )
                self.log.info(f"schedd status: {pformat(res)}")
                if res:
                    if res[0]["JobStatus"] not in (3, 4):
                        seen_jobs.append(jobid)
                    if res[0]["JobStatus"] == 1:
                        first_seen = start_time

        # Are we done?
        if not seen_jobs:
            # Happy days

            self.log.info(
                "All %d jobs in list exited after %.1f seconds.",
                jobcount,
                time.time() - first_seen,
                extra={
                    "stat-time-max": os_stat_profiler.max,
                    "stat-time-mean": os_stat_profiler.mean,
                },
            )

            rw.send_to(
                "any",
                {"jobs-expected": jobcount, "jobs-seen": seen_jobs},
                transaction=txn,
            )
            rw.send_to(
                "finally",
                {
                    "jobs-expected": jobcount,
                    "jobs-seen": seen_jobs,
                    "success": True,
                },
                transaction=txn,
            )

            rw.transport.transaction_commit(txn)
            return

        message_delay = rw.recipe_step["parameters"].get("burst-wait")
        if seen_jobs:
            # Check timeout conditions.
            timeout = rw.recipe_step["parameters"].get("timeout", 3600)
            timed_out = (first_seen + timeout) < time.time()
            runtime = time.time() - first_seen
            if timed_out:
                # HTcondor watch operation has timed out. Put timed out job into Hold state.
                act_result = schedd.act(
                    htcondor.JobAction.Hold,
                    " && ".join([f"ClusterId == {jobid}" for jobid in seen_jobs]),
                    reason=f"Job timed out after {runtime} seconds",
                )
                self.log.info(f"schedd act response: {pformat(act_result)}")
                # Report all timeouts as warnings unless the recipe specifies otherwise
                timeoutlog = self.log.warning
                if rw.recipe_step["parameters"].get("log-timeout-as-info"):
                    timeoutlog = self.log.info

                timeoutlog(
                    "HTCondorwatcher for jobs %s timed out after %.1f seconds (%d of %d jobs found after %.1f seconds)",
                    pformat(seen_jobs),
                    runtime,
                    len(seen_jobs),
                    jobcount,
                    runtime,
                    extra={
                        "stat-time-max": os_stat_profiler.max,
                        "stat-time-mean": os_stat_profiler.mean,
                    },
                )

                # Notify for timeout
                rw.send_to(
                    "timeout",
                    {
                        "jobid": seen_jobs,
                        "success": False,
                    },
                    transaction=txn,
                )
                # Notify for 'any' target if any job was seen
                if seen_jobs:
                    rw.send_to(
                        "any",
                        {
                            "jobid": seen_jobs,
                            "success": False,
                        },
                        transaction=txn,
                    )

                # Notify for 'finally' outcome
                rw.send_to(
                    "finally",
                    {
                        "jobid": seen_jobs,
                        "success": False,
                    },
                    transaction=txn,
                )
                # Stop processing message
                rw.transport.transaction_commit(txn)
                return

            # If no timeouts are triggered, set a minimum waiting time.
            if message_delay:
                message_delay = max(1, message_delay)
            else:
                message_delay = 1
            self.log.debug(
                (
                    "No further jobs in list finished after a total time of {time:.1f} seconds\n"
                    "{jobs_seen} of {jobs_total} still running."
                ).format(
                    time=time.time() - first_seen,
                    jobs_seen=len(seen_jobs),
                    jobs_total=jobcount,
                ),
                extra={
                    "stat-time-max": os_stat_profiler.max,
                    "stat-time-mean": os_stat_profiler.mean,
                },
            )

        # Send results to myself for next round of processing
        rw.checkpoint(
            {"jobid": joblist, "first-seen": first_seen},
            delay=message_delay,
            transaction=txn,
        )
        rw.transport.transaction_commit(txn)
