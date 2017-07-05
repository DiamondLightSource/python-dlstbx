from __future__ import absolute_import, division
from dlstbx.system_test.common import CommonSystemTest

class ActiveMQ(CommonSystemTest):
  '''Connect to messaging server and send a message to myself.'''

  def test_loopback_message(self):
    self.send_message(
      queue='transient.system_test.' + self.guid,
      message='loopback ' + self.guid,
    )

    self.expect_message(
      queue='transient.system_test.' + self.guid,
      message='loopback ' + self.guid,
      timeout=10,
    )

if __name__ == "__main__":
  ActiveMQ().validate()
