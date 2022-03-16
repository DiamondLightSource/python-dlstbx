from __future__ import annotations

from dlstbx.system_test.common import CommonSystemTest


class Transport(CommonSystemTest):
    """Connect to messaging server and send a message to myself."""

    def test_loopback_message(self):
        self.send_message(queue=self.target_queue, message="loopback " + self.guid)

        self.expect_message(
            queue=self.target_queue,
            message="loopback " + self.guid,
            timeout=10,
        )
