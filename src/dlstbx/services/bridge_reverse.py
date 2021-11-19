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
        "ispyb_pia": "ispyb_pia",
        "notify_gda": "notify_gda",
    }

    def initializing(self):
        self.log.debug("Reverse Bridge service starting")

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
                self.log.info(f"message {message} sent to {send_to}")
            except workflows.Disconnected:
                self.log.error(
                    f"Connection to ActiveMQ failed: trying to send to {send_to}"
                )
                raise
        else:
            self.log.error("No destination queue specified")
            self._transport.nack(header)
