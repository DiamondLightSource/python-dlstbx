import dlstbx.workflow.services
import time

class Waiter(dlstbx.workflow.services.Service):
  def __init__(self, *args, **kwargs):
    '''Pass on arguments to baseclass constructor.'''
    super(Waiter, self).__init__(*args, **kwargs)

  def initialize(self):
    '''Register handling function for 'stuff' messages.'''
    self._register('stuff', self.stuff_handler)

  def stuff_handler(self, *args, **kwargs):
    self.update_status('Processing stuff [1/3]')
    time.sleep(3)
    self.update_status('Processing stuff [2/3]')
    time.sleep(8)
    self.update_status('Processing stuff [3/3]')
    time.sleep(4)
    self.update_status('Completed processing stuff')
