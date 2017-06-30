#
# dlstbx.wrap
#   Wraps a command so that its status can be tracked in zocalo
#

from __future__ import division, absolute_import
from dlstbx import enable_graylog
import dlstbx.util
from dlstbx.util.colorstreamhandler import ColorStreamHandler
from dlstbx.util.version import dlstbx_version
import glob
import json
import logging
import os
from optparse import OptionParser, SUPPRESS_HELP
import shutil
import sys
import time
import threading
import workflows
import workflows.services.common_service
import workflows.transport
from workflows.transport.stomp_transport import StompTransport

class StatusNotifications(threading.Thread):
  def __init__(self, send_function):
    super(StatusNotifications, self).__init__()
    self.daemon = True
    self.lock = threading.Condition(threading.Lock())
    self.send_status = send_function
    self.status_dict = {
      'host': 'self.__hostid',
      'service': 'self._service_name',
      'workflows': workflows.version(),
    }
    for env in ('SGE_CELL', 'JOB_ID'):
      if env in os.environ:
        self.status_dict['cluster_' + env] = os.environ[env]
    self.set_status(workflows.services.common_service.Status.STARTING)
    self._keep_running = True

  def get_status(self):
    '''Returns a dictionary containing all relevant status information to be
       broadcast across the network.'''
    return self.status_dict

  def set_status(self, status):
    self.lock.acquire()
    self.status_dict['status'], self.status_dict['statustext'] = status.intval, status.description
    self.lock.notify()
    self.lock.release()

  def shutdown(self):
    self._keep_running = False

  def run(self):
    self.lock.acquire()
    self.send_status(self.get_status())
    while self._keep_running:
      self.lock.wait(3)
      self.send_status(self.get_status())
    self.lock.release()

def run(cmdline_args):
  # Enable logging to console
  console = ColorStreamHandler()
  console.setLevel(logging.INFO)
  logging.getLogger().setLevel(logging.WARN)
  logging.getLogger().addHandler(console)

  logging.getLogger('dials').setLevel(logging.INFO)
  logging.getLogger('dlstbx').setLevel(logging.INFO)
  logging.getLogger('workflows').setLevel(logging.INFO)
  logging.getLogger('xia2').setLevel(logging.INFO)
  log = logging.getLogger('dlstbx.wrap')

  # Set up stomp defaults
  default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'
  if '--test' in cmdline_args:
    default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg'
  StompTransport.load_configuration_file(default_configuration)

  # Set up parser
  parser = OptionParser(
    usage='dlstbx.wrap wrapper [options]'
  )
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-t", "--transport", dest="transport", metavar="TRN",
    default="StompTransport",
    help="Transport mechanism. Known mechanisms: " + \
         ", ".join(workflows.transport.get_known_transports()) + \
         " (default: %default)")
  workflows.transport.add_command_line_options(parser)
  parser.add_option("--test", action="store_true",
                    help="Run in ActiveMQ testing namespace (zocdev)")
  parser.add_option("--live", action="store_true",
                    help="Run in ActiveMQ live namespace (zocalo, default)")

  # Parse command line arguments
  (options, args) = parser.parse_args(cmdline_args)

  # Instantiate specific wrapper
  if len(args) > 1:
    print "Exactly one wrapper needs to be specified."
    sys.exit(1)
  elif len(args) < 1:
    print "No wrapper has been specified."
    sys.exit(1)

  class wrapper(object):
    def run(self):
      print "Starting task"
      import time
      time.sleep(10)
      print "Done"
  instance = wrapper

  # Enable logging to graylog
  enable_graylog()

  # Connect to transport and start sending notifications
  transport = workflows.transport.lookup(options.transport)()
  transport.connect()
  st = StatusNotifications(transport.broadcast_status)
  st.start()

  instance = instance()

  st.set_status(workflows.services.common_service.Status.PROCESSING)

  try:
    instance.run()
    st.set_status(workflows.services.common_service.Status.END)
  except KeyboardInterrupt:
    print("\nShutdown via Ctrl+C")

  st.set_status(workflows.services.common_service.Status.END)
  st.shutdown()
  st.join()

if __name__ == '__main__':
  logging.basicConfig(level=logging.DEBUG)
  run(sys.argv[1:])
