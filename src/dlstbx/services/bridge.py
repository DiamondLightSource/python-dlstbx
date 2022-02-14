from __future__ import annotations

from functools import partial

import workflows
import zocalo.configuration
from workflows.services.common_service import CommonService
from workflows.transport.pika_transport import PikaTransport


class DLSBridge(CommonService):
    """A service that takes ActiveMQ messages and moves them to RabbitMQ."""

    # Human readable service name
    _service_name = "DLS MQ Bridge"

    # Logger name
    _logger_name = "dlstbx.services.bridge"

    def initializing(self):
        self.log.info("Bridge service starting")
        self.pika_transport = PikaTransport()
        self.pika_transport.connect()

        zc = zocalo.configuration.from_file()
        zc.activate()
        queues = zc.storage.get("zocalo.bridge.queues", {})
        self.log.info(f"Subscribing to {queues=}")
        for queue in queues:
            self._transport.subscribe(
                queue,
                partial(self.receive_msg, args=(queues[queue])),
                acknowledgement=True,
            )

    def receive_msg(self, header, message, args):
        send_to = args
        if send_to:
            try:
                self.pika_transport.send(
                    send_to,
                    message,
                    headers=header,
                )
                self._transport.ack(header)
            except workflows.Disconnected:
                self.log.error(
                    f"Connection to RabbitMQ failed: trying to send to {send_to}",
                    exc_info=True,
                )
                raise
        else:
            self.log.error("No destination queue specified")
            self._transport.nack(header)
