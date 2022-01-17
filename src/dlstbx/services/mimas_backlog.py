from __future__ import annotations

import logging
import threading
import time

import workflows.recipe
from workflows.services.common_service import CommonService


class DLSMimasBacklog(CommonService):
    """
    A service to monitor the mimas.held backlog queue and drip-feed them into
    the live queue as long as there isn't a cluster backlog.
    """

    _service_name = "DLS Mimas Backlog"
    _logger_name = "dlstbx.services.mimas_backlog"

    def initializing(self):
        """Subscribe to mimas.held and transient.statistics.cluster"""
        self.log.info("MimasBacklog service starting up")

        self._max_jobs_waiting = 60
        self._jobs_waiting = self._max_jobs_waiting
        self._last_cluster_update = time.time()
        self._held_data = None
        self._lock = threading.Lock()

        # Subscribe to the mimas.held queue, which contains the held mimas
        # recipes we would like to drip-feed to the dispatcher
        workflows.recipe.wrap_subscribe(
            self._transport,
            "mimas.held",
            self.on_mimas_held,
            acknowledgement=True,
            exclusive=True,
            log_extender=self.extend_log,
        )

        # Subscribe to the transient.statistics.cluster topic, which we will
        # examine to determine the number of waiting jobs
        self._transport.subscribe_broadcast(
            "transient.statistics.cluster",
            self.on_statistics_cluster,
        )

    def on_statistics_cluster(self, header, message):
        """
        Examine the message to determine number of waiting jobs.

        If there are fewer than 10 waiting jobs, and we have a stored held
        message, then forward this held message to the trigger step.

        We are only interested in the "live" cluster for now. We are only
        concerned about the number of waiting jobs in high.q or medium.q.
        """
        if (
            message["statistic-cluster"] == "live"
            and message["statistic"] == "waiting-jobs-per-queue"
        ):
            with self._lock:
                self._last_cluster_update = time.time()
                self._jobs_waiting = message["high.q"] + message["medium.q"]
                self.log.log(
                    logging.INFO if self._jobs_waiting else logging.DEBUG,
                    f"Jobs waiting: {self._jobs_waiting}",
                )
                if self._jobs_waiting < self._max_jobs_waiting and self._held_data:
                    self.forward_message(*self._held_data)
                    self._held_data = None

    def on_mimas_held(self, rw, header, message):
        """
        Forward message to trigger if there are fewer than 10 waiting jobs.

        Otherwise, store this message until there are fewer waiting jobs.
        """
        self.log.debug(f"Jobs waiting: {self._jobs_waiting}")
        with self._lock:
            assert not self._held_data, "unexpectedly received multiple messages"
            if self._jobs_waiting < self._max_jobs_waiting:
                if self._last_cluster_update > time.time() - 300:
                    self.forward_message(rw, header, message)
                else:
                    self.log.warning(
                        "Not heard from the cluster for over 5 minutes. Holding jobs."
                    )
                    self._held_data = (rw, header, message)
            else:
                self._held_data = (rw, header, message)

    def forward_message(self, rw, header, message):
        """
        Forward the held message to the trigger step.

        Acknowledge receipt of the message, and increment the jobs_waiting
        counter.
        """
        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin()
        rw.transport.ack(header, transaction=txn)

        rw.send(message, transaction=txn)
        # Commit transaction
        rw.transport.transaction_commit(txn)
        self._jobs_waiting += 1
        self.log.info(f"Sent message to trigger: {message}")
