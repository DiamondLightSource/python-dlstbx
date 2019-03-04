from __future__ import absolute_import, division, print_function

import time

from workflows.services.common_service import CommonService


class LoadReceiver(CommonService):
    """service consuming messages as fast as possible."""

    # Human readable service name
    _service_name = "Load receiver"

    # Logger name
    _logger_name = "dlstbx.services.load_receiver"

    def initializing(self):
        """Subscribe to channels."""
        self.interval = time.time() + 5
        self.count = 0
        self._transport.subscribe("destination", self.consume_message)
        self._transport.subscribe("transient.destination", self.consume_message)
        self._register_idle(1, self.print_stats)

    def consume_message(self, header, message):
        """Consume a message"""
        if self.interval < time.time():
            self.log.info(
                "Received %5d messages in 5 seconds = %7.1f/s",
                self.count,
                self.count / 5,
            )
            self.interval = time.time() + 5
            self.count = 1
        else:
            self.count = self.count + 1

    def print_stats(self):
        """Continue to print statistics when idle"""
        if self.interval < time.time():
            self.log.info(
                "Received %5d messages in 5 seconds = %7.1f/s",
                self.count,
                self.count / 5,
            )
            self.interval = time.time() + 5
            self.count = 0
