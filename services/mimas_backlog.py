import threading
import workflows.recipe
from workflows.services.common_service import CommonService


class DLSMimasBacklog(CommonService):
    """
    A service to monitor the mimas.held backlog queue and drip-feed them into
    the live queue as long as there isn't a cluster backlog.
    """

    _service_name = "DLSMimasBacklog"
    _logger_name = "dlstbx.services.mimas_backlog"

    def initializing(self):
        """Subscribe to mimas.held and transient.statistics.cluster"""
        self.log.info("MimasBacklog service starting up")

        self._cluster_free_slots = 0
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
        # examine to determine the number of free slots
        self._transport.subscribe_broadcast(
            "transient.statistics.cluster", self.on_statistics_cluster,
        )

    def on_statistics_cluster(self, header, message):
        """
        Examine the statistics cluster message to determine number of free slots.

        Limit the number of free slots to 10, so that we don't dump too many
        held messages at once, since we only get updated cluster statistics
        intermittently.

        If we have free slots, and we have a stored held message, then forward
        this held message to the trigger step.

        We are only interested in the "live" cluster for now. Since we are
        currently only holding messages that would have been sent to medium.q,
        we only care about number of free slots in this queue
        """
        if (
            message["statistic-cluster"] == "live"
            and message["statistic"] == "utilization"
        ):
            free_slots = message["cpu"]["free_for_medium"]
            self.log.debug(f"free_for_medium: {free_slots}")
            with self._lock:
                self._cluster_free_slots = min(10, free_slots)
                if self._cluster_free_slots > 0 and self._held_data:
                    self.forward_message(*self._held_data)
                    self._held_data = None

    def on_mimas_held(self, rw, header, message):
        """
        Forward message to trigger if we have free cluster slots available.

        Otherwise, store this message until we do have free slots.
        """
        self.log.debug(f"Free slots: {self._cluster_free_slots}")
        with self._lock:
            if self._cluster_free_slots > 0:
                self.forward_message(rw, header, message)
            else:
                assert not self._held_data, "unexpectedly received multiple messages"
                self._held_data = (rw, header, message)

    def forward_message(self, rw, header, message):
        """
        Forward the held message to the trigger step.

        Acknowledge receipt of the message, and decrement the free-slot counter.
        """
        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin()
        rw.transport.ack(header, transaction=txn)

        rw.send(message, transaction=txn)
        # Commit transaction
        rw.transport.transaction_commit(txn)
        self._cluster_free_slots -= 1
        self.log.info(f"Sent message to trigger: {message}")
