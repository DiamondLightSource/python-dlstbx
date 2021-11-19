from functools import partial

import workflows
from workflows.services.common_service import CommonService
from workflows.transport.pika_transport import PikaTransport


class DLSBridge(CommonService):
    """A service that takes ActiveMQ messages and moves them to RabbitMQ."""

    # Human readable service name
    _service_name = "DLS MQ Bridge"

    # Logger name
    _logger_name = "dlstbx.services.bridge"

    queues = {
        "rabbit.pia": "per_image_analysis",
    }

    def initializing(self):
        self.log.debug("Bridge service starting")
        self.pika_transport = PikaTransport()
        self.pika_transport.connect()

        print("initialising DLSBridge service")
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
                self.pika_transport.send(
                    send_to,
                    message,
                    headers=header,
                )
                self._transport.ack(header)
                self.log.info(f"message {message} sent to {send_to}")
            except workflows.Disconnected:
                self.log.error(
                    f"Connection to RabbitMQ failed: trying to send to {send_to}"
                )
                raise
        else:
            self.log.error("No destination queue specified")
            self._transport.nack(header)
