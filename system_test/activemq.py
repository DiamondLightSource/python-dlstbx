from __future__ import absolute_import, division
from dlstbx.system_test.common import CommonSystemTest

class ActiveMQ(CommonSystemTest):
  '''Connect to messaging server and send a message to myself.'''

  def test_loopback_message(self):
    self.send_message(
      queue=self.apply_parameters('transient.system_test.{guid}'),
      message=self.apply_parameters('loopback {guid}'),
    )

    self.expect_message(
      queue=self.apply_parameters('transient.system_test.{guid}'),
      message=self.apply_parameters('loopback {guid}'),
      timeout=3,
    )

if __name__ == "__main__":
  ActiveMQ().validate()
