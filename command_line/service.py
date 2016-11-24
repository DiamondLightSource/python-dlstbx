#
# dlstbx.service
#   Starts a workflow service
#

from __future__ import division
from dlstbx import enable_graylog
from dlstbx.util.colorstreamhandler import ColorStreamHandler
from dlstbx.util.version import dlstbx_version
import logging
import os.path
import sys
import workflows
import workflows.contrib.start_service
import workflows.logging

class DLSTBXServiceStarter(workflows.contrib.start_service.ServiceStarter):
  __frontendref = None

  def setup_logging(self):
    '''Initialize common logging framework. Everything is logged to central
       graylog server. Depending on setting messages of DEBUG or INFO and higher
       go to console.'''
    logger = logging.getLogger()
    logger.setLevel(logging.WARN)

    # Enable logging to console
    self.console = ColorStreamHandler()
    self.console.setLevel(logging.INFO)
    logger.addHandler(self.console)

    logging.getLogger('dials').setLevel(logging.DEBUG)
    logging.getLogger('dlstbx').setLevel(logging.DEBUG)
    logging.getLogger('workflows').setLevel(logging.INFO)
    logging.getLogger('xia2').setLevel(logging.DEBUG)

    self.log = logging.getLogger('dlstbx.service')
    self.log.setLevel(logging.DEBUG)

    # Enable logging to graylog
    enable_graylog()

  def __init__(self):
    # initialize logging
    self.setup_logging()

    self.log.debug('Loading dlstbx workflows plugins')

    dlstbx = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    workflows.load_plugins([os.path.join(dlstbx, 'services')])

    self.log.debug('Loading dlstbx credentials')

    # override default stomp host
    from workflows.transport.stomp_transport import StompTransport
    try:
      StompTransport.load_configuration_file(
        '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg')
    except workflows.WorkflowsError, e:
      self.log.warn(e)

  def on_parser_preparation(self, parser):
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                      default=False, help="Show debug output")
    parser.add_option("-d", "--debug", dest="debug", action="store_true",
                      default=False, help="Set debug log level for workflows")
    self.log.debug('Launching ' + str(sys.argv))

  def on_parsing(self, options, args):
    if options.verbose:
      self.console.setLevel(logging.DEBUG)
    if options.debug:
      logging.getLogger('workflows').setLevel(logging.DEBUG)

  def on_transport_preparation(self, transport):
    self.log.info('Attaching ActiveMQ logging to transport')
    def logging_call(record):
      if transport.is_connected():
        try:
          record = record.__dict__['records']
        except:
          record = record.__dict__
        transport.broadcast('transient.log', record)
    logging.getLogger().addHandler(workflows.logging.CallbackHandler(logging_call))

if __name__ == '__main__':
  DLSTBXServiceStarter().run(program_name='dlstbx.service',
                             version=dlstbx_version(),
                             transport_command_prefix='transient.command.')
