from __future__ import annotations

from functools import partial

import workflows
from workflows.services.common_service import CommonService
from workflows.transport.stomp_transport import StompTransport


class DLSReverseBridge(CommonService):
    """A service that takes RabbitMQ messages and moves them to ActiveMQ."""

    # Human readable service name
    _service_name = "DLS MQ Reverse Bridge"

    # Logger name
    _logger_name = "dlstbx.services.bridge_reverse"

    queues = {
        "reduce.xray_centering": "reduce.xray_centering",
    }

    def initializing(self):
        self.log.info("Reverse Bridge service starting")

        self.stomp_transport = StompTransport()

        print("initialising DLSReverseBridge service")
        self.stomp_transport.connect()
        for queue in self.queues:
            self._transport.subscribe(
                queue,
                partial(self.receive_msg, args=(self.queues[queue])),
                acknowledgement=True,
            )

    def receive_msg(self, header, message, args):
        send_to = args
        if send_to:
            try:
                self.stomp_transport.send(
                    send_to,
                    message,
                    headers=header,
                )

                self._transport.ack(header)
            except workflows.Disconnected:
                self.log.error(
                    f"Connection to ActiveMQ failed: trying to send to {send_to}",
                    exc_info=True,
                )
                raise
        else:
            self.log.error("No destination queue specified")
            self._transport.nack(header)
