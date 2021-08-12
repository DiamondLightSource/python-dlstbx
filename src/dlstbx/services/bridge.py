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

    queues = {"bridge.test": "bridge.test"}

    def initializing(self):
        self.log.debug("Bridge service starting")
        default_configuration = (
            "/dls_sw/apps/zocalo/secrets/rabbitmq/credentials-zocalo.cfg"
        )
        PikaTransport.load_configuration_file(default_configuration)
        self.pika_transport = PikaTransport()

        print("initialising DLSBridge service")
        for queue in self.queues:
            self._transport.subscribe(
                queue,
                partial(self.receive_msg, args=(self.queues[queue])),
            )

    def receive_msg(self, header, message, args):
        send_to = args
        if send_to:
            self.pika_transport.connect()
            try:
                self.pika_transport.send(
                    send_to,
                    message,
                    headers=header,
                )
                self.pika_transport.ack(header)
            except workflows.Disconnected:
                self.log.error("Connection to RabbitMQ failed")
                self.pika_transport.nack(header)
        else:
            self.log.error("No destination queue specified")
            self.pika_transport.nack(header)
