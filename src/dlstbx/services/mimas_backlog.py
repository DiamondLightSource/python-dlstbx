from __future__ import annotations

import logging
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

        self._message_delay = 30
        self._jobs_waiting = {"slurm": 60, "iris": 3000}
        self._last_cluster_update = {"slurm": time.time(), "iris": time.time()}

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

        We are interested in the "slurm" and "iris" clusters for now. We are
        only concerned about the number of pending gda2 jobs.
        """
        for statistic_cluster in ("slurm", "iris"):
            if (
                message["statistic-cluster"] == statistic_cluster
                and message["statistic"] == "job-states"
            ):
                self._last_cluster_update[statistic_cluster] = time.time()
                self._jobs_waiting[statistic_cluster] = message.get("PENDING", 0)
                self.log.log(
                    logging.INFO
                    if self._jobs_waiting[statistic_cluster]
                    else logging.DEBUG,
                    f"Jobs waiting on {statistic_cluster} cluster: {self._jobs_waiting[statistic_cluster]}\n",
                )

    def on_mimas_held(self, rw, header, message):
        """
        Forward message to trigger if number of waiting jobs doesn't exceed
        the predefined threshold.
        """
        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        statistic_cluster = message["parameters"].get("statistic-cluster", "slurm")
        try:
            max_jobs_waiting = self.config.storage.get(
                "max_jobs_waiting", {"slurm": 60, "iris": 3000}
            )
            timeout = self.config.storage.get("timeout", 300)
        except AttributeError:
            max_jobs_waiting = {"slurm": 60, "iris": 3000}
            timeout = 300
        self.log.debug(
            f"Jobs waiting on {statistic_cluster} cluster: {self._jobs_waiting[statistic_cluster]}\n"
        )

        if self._jobs_waiting[statistic_cluster] < max_jobs_waiting[statistic_cluster]:
            if self._last_cluster_update[statistic_cluster] > time.time() - timeout:
                rw.send(message, transaction=txn)
                self._jobs_waiting[statistic_cluster] += 1
                self.log.info(f"Sent message to trigger: {message}")
            else:
                self.log.warning(
                    f"Not heard from {statistic_cluster} cluster for over 5 minutes. Holding jobs."
                )
                rw.checkpoint(
                    message,
                    delay=self._message_delay,
                    transaction=txn,
                )
        else:
            rw.checkpoint(
                message,
                delay=self._message_delay,
                transaction=txn,
            )

        # Commit transaction
        rw.transport.transaction_commit(txn)
