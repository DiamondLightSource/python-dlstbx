import time

from workflows.services.common_service import CommonService


class LoadProducer(CommonService):
    """A service creating messages as quickly as possible."""

    # Human readable service name
    _service_name = "Load Producer"

    # Logger name
    _logger_name = "dlstbx.services.load_producer"

    def initializing(self):
        """Generate messages."""
        counter = 0
        interval = time.time() + 5
        content = "X" * 1024
        while True:
            counter += 1
            self._transport.send(
                "transient.destination",
                #     self._transport.send("destination",
                content,
            )
            if interval < time.time():
                self.log.info(
                    "Produced %5d 1K messages in 5 seconds = %7.1f/s",
                    counter,
                    counter / 5,
                )
                counter = 0
                interval = time.time() + 5
