from __future__ import absolute_import, division, print_function

import time

import confluent_kafka
import msgpack
import workflows.recipe
from workflows.services.common_service import CommonService


class DLSStreamDispatch(CommonService):
    """A service that triggers actions running on stream data."""

    # Human readable service name
    _service_name = "DLS Stream Dispatcher"

    # Logger name
    _logger_name = "dlstbx.services.stream_dispatch"

    def initializing(self):
        """Subscribe to the stream_analysis queue. Received messages must be acknowledged."""
        workflows.recipe.wrap_subscribe(
            self._transport,
            "stream_dispatch",
            self.stream_dispatch,
            acknowledgement=True,
            log_extender=self.extend_log,
        )
        self.kafka = confluent_kafka.Producer({"bootstrap.servers": "ws133"})

    def stream_dispatch(self, rw, header, message):
        """Set up stream analysis for specified stream."""

        # Identify the data collection ID
        dcid = rw.recipe_step.get("dcid")
        if not dcid or not str(dcid).isdigit():
            self.log.error(
                "Missing or invalid data collection ID %r", dcid, exc_info=True
            )
            rw.transport.nack(header)
            return

        framecount = rw.recipe_step.get("framecount")
        if not framecount or not str(framecount).isdigit():
            self.log.error(
                "Missing or invalid framecount %r", framecount, exc_info=True
            )
            rw.transport.nack(header)
            return
        framecount = int(framecount)

        activity = rw.recipe_step.get("activity")
        if not activity:
            self.log.error("Activity not defined", exc_info=True)
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
            masgpack.packb(kafka_update, use_bin_type=True),
            key=dcid,
        )

        # Produce per-offset messages.
        # These may or may not correspond to frames, as EIGER streams do not guarantee ordering.
        for offset in range(framecount):
            stream_cmd = {"dcid": dcid, "activity": activity, "offset": offset}
            rw.send(frame_cmd, transaction=txn)

        # And dispatch
        self.kafka.flush()
        rw.transport.transaction_commit(txn)
        self.log.info(
            "Stream analysis dispatched for DCID %d with activity %s on %d images",
            dcid,
            activity,
            framecount,
        )
