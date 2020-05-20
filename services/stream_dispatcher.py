import time

import confluent_kafka
import msgpack
import workflows.recipe
from workflows.services.common_service import CommonService


class DLSStreamDispatcher(CommonService):
    """A service that triggers actions running on stream data."""

    # Human readable service name
    _service_name = "DLS Stream Dispatcher"

    # Logger name
    _logger_name = "dlstbx.services.stream_dispatcher"

    def initializing(self):
        """Subscribe to the stream_analysis queue. Received messages must be acknowledged."""
        workflows.recipe.wrap_subscribe(
            self._transport,
            "stream_dispatcher",
            self.stream_dispatch,
            acknowledgement=True,
            log_extender=self.extend_log,
        )
        self.kafka = confluent_kafka.Producer({"bootstrap.servers": "ws133"})

    def stream_dispatch(self, rw, header, message):
        """Set up stream analysis for specified stream."""

        # Load and sanity-check parameters
        params = rw.recipe_step.get("parameters", {})
        try:
            dcid = int(params.get("dcid"))
        except (ValueError, TypeError):
            dcid = 0
        if dcid <= 0:
            self.log.error("Missing or invalid data collection ID")
            rw.transport.nack(header)
            return
        try:
            framecount = int(params.get("framecount"))
        except (ValueError, TypeError):
            framecount = 0
        if framecount <= 0:
            self.log.error("Missing or invalid framecount")
            rw.transport.nack(header)
            return
        activity = params.get("activity")
        if not activity:
            self.log.error("Activity not defined")
            rw.transport.nack(header)
            return

        self.log.debug(
            "Dispatching stream analysis for DCID %d with activity %s on %d images",
            dcid,
            activity,
            framecount,
        )

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin()
        rw.transport.ack(header, transaction=txn)

        # Record activity on stream in Kafka
        kafka_update = {"DCID": dcid, "activity": activity, "timestamp": time.time()}
        self.kafka.produce(
            "hoggery.activity",
            msgpack.packb(kafka_update, use_bin_type=True),
            key=str(dcid),
        )

        # Produce per-offset messages.
        # These may or may not correspond to frames, as EIGER streams do not guarantee ordering.
        for offset in range(framecount):
            rw.send(
                {"dcid": dcid, "activity": activity, "offset": offset}, transaction=txn
            )

        # And dispatch
        self.kafka.flush()
        rw.transport.transaction_commit(txn)
        self.log.info(
            "Stream analysis dispatched for DCID %r with activity %s on %d images",
            dcid,
            activity,
            framecount,
        )
