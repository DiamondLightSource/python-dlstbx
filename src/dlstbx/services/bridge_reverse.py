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
        "bridge.test": "bridge.test",
        "transient.destination": "transient.destination",
    }

    def initializing(self):
        self.log.debug("Reverse Bridge service starting")
        default_configuration = "/dls_sw/apps/zocalo/secrets/activemq-credentials.yml"
        StompTransport.load_configuration_file(default_configuration)
        self.stomp_transport = StompTransport()

        print("initialising DLSReverseBridge service")
        for queue in self.queues:
            self._transport.subscribe(
                queue,
                partial(self.receive_msg, args=(self.queues[queue])),
            )

    def receive_msg(self, header, message, args):
        send_to = args
        if send_to:
            self.stomp_transport.connect()
            try:
                self.stomp_transport.send(
                    send_to,
                    message,
                    headers=header,
                )
                self.stomp_transport.ack(header)
                self.log.info(f"message {message} sent to {send_to}")
            except workflows.Disconnected:
                self.log.error(
                    f"Connection to ActiveMQ failed: trying to send to {send_to}"
                )
                self.stomp_transport.nack(header)
        else:
            self.log.error("No destination queue specified")
            self.stomp_transport.nack(header)
