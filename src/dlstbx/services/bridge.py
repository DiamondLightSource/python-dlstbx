from workflows.services.common_service import CommonService
from workflows.transport.pika_transport import PikaTransport


class DLSBridge(CommonService):
    """A service that takes ActiveMQ messages and moves them to RabbitMQ."""

    # Human readable service name
    _service_name = "DLS MQ Bridge"

    # Logger name
    _logger_name = "dlstbx.services.bridge"

    queues = {}
    pika_transport = PikaTransport()

    def initializing(self):
        self.log.debug("Bridge service starting")
        for queue in self.queues.items():
            self._transport.subscribe(
                self._transport,
                queue,
                self.receive_msg,
            )

    def receive_msg(self, header, message):
        if "ConsumerInfo" in message:
            self.pika_transport.connect()
            self.pika_transport.broadcast(
                self.queues[message["ConsumerInfo"]["destination"]["string"]],
                message,
                headers=header,
            )
