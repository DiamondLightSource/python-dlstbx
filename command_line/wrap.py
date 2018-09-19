#
# dlstbx.wrap
#   Wraps a command so that its status can be tracked in zocalo
#

from __future__ import absolute_import, division, print_function

import json
import logging
import os
import sys
import threading
from optparse import SUPPRESS_HELP, OptionParser
import pkg_resources

import workflows
import workflows.recipe.wrapper
import workflows.services.common_service
import workflows.transport
import workflows.util
from dlstbx import enable_graylog
from dlstbx.util.colorstreamhandler import ColorStreamHandler
from dlstbx.util.version import dlstbx_version
from workflows.transport.stomp_transport import StompTransport

class StatusNotifications(threading.Thread):
  def __init__(self, send_function, taskname):
    super(StatusNotifications, self).__init__(name="zocalo status notification")
    self.daemon = True
    self.lock = threading.Condition(threading.Lock())
    self.send_status = send_function
    self.status_dict = {
      'host': workflows.util.generate_unique_host_id(),
      'task': taskname,
      'dlstbx': dlstbx_version(),
      'workflows': workflows.version(),
    }
    for env in ('SGE_CELL', 'JOB_ID'):
      if env in os.environ:
        self.status_dict['cluster_' + env] = os.environ[env]
    self.set_status(workflows.services.common_service.Status.STARTING)
    self._keep_running = True
    self.start()

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
  logging.getLogger('dials').setLevel(logging.INFO)
  logging.getLogger('dlstbx').setLevel(logging.INFO)
  logging.getLogger('workflows').setLevel(logging.INFO)
  logging.getLogger('xia2').setLevel(logging.INFO)
  logging.getLogger().setLevel(logging.WARN)
  logging.getLogger().addHandler(console)
  log = logging.getLogger('dlstbx.wrap')

  # Set up stomp defaults
  default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'
  if '--test' in cmdline_args:
    default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg'
  StompTransport.load_configuration_file(default_configuration)

  known_wrappers = { e.name: e.load for e in pkg_resources.iter_entry_points('dlstbx.wrappers') }

  # Set up parser
  parser = OptionParser(
    usage='dlstbx.wrap [options]'
  )
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)

  parser.add_option("--wrap", action="store", dest="wrapper", type="choice",
                    metavar="WRAP", default=None,
                    choices=list(known_wrappers),
                    help="Object to be wrapped (valid choices: %s)" % ", ".join(known_wrappers))
  parser.add_option("--recipewrapper", action="store", dest="recipewrapper",
                    metavar="RW", default=None,
                    help="A serialized recipe wrapper file " \
                         "for downstream communication")

  parser.add_option("--test", action="store_true",
                    help="Run in ActiveMQ testing namespace (zocdev)")
  parser.add_option("--live", action="store_true",
                    help="Run in ActiveMQ live namespace (zocalo, default)")

  parser.add_option("-t", "--transport", dest="transport", metavar="TRN",
    default="StompTransport",
    help="Transport mechanism. Known mechanisms: " + \
         ", ".join(workflows.transport.get_known_transports()) + \
         " (default: %default)")
  workflows.transport.add_command_line_options(parser)

  # Parse command line arguments
  (options, args) = parser.parse_args(cmdline_args)

  # Instantiate specific wrapper
  if not options.wrapper:
    print("A wrapper object must be specified.")
    sys.exit(1)

  # Enable logging to graylog
  graylog_handler = enable_graylog()
  log.info('Starting wrapper for %s with recipewrapper file %s', options.wrapper, options.recipewrapper)

  # Connect to transport and start sending notifications
  transport = workflows.transport.lookup(options.transport)()
  transport.connect()
  st = StatusNotifications(transport.broadcast_status, options.wrapper)

  # Instantiate chosen wrapper
  instance = known_wrappers[options.wrapper]()()

  # If specified, read in a serialized recipewrapper
  if options.recipewrapper:
    with open(options.recipewrapper, 'r') as fh:
      recwrap = workflows.recipe.wrapper.RecipeWrapper(
          message=json.load(fh),
          transport=transport,
      )
    instance.set_recipe_wrapper(recwrap)

    if recwrap.environment.get('ID'):
      # If recipe ID available then include that in all future log messages
      class ContextFilter(logging.Filter):
        def filter(self, record):
          record.recipe_ID = recwrap.environment['ID']
          return True
      graylog_handler.addFilter(ContextFilter())

  instance.prepare('Starting processing')

  st.set_status(workflows.services.common_service.Status.PROCESSING)
  log.info('Setup complete, starting processing')

  try:
    if instance.run():
      log.info('successfully finished processing')
      instance.success('Finished processing')
    else:
      log.info('processing failed')
      instance.failure('Processing failed')
    st.set_status(workflows.services.common_service.Status.END)
  except KeyboardInterrupt:
    log.info('Shutdown via Ctrl+C')
    st.set_status(workflows.services.common_service.Status.END)
  except Exception as e:
    log.error(str(e), exc_info=True)
    instance.failure(e)
    st.set_status(workflows.services.common_service.Status.ERROR)

  instance.done('Finished processing')

  st.shutdown()
  st.join()
  log.debug('Terminating')

if __name__ == '__main__':
  run(sys.argv[1:])
