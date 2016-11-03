#
# dlstbx.service
#   Starts a workflow service
#

from __future__ import division
from dlstbx import enable_graylog
from dlstbx.util.version import dlstbx_version
import logging
import multiprocessing
import os.path
import sys
import workflows
import workflows.contrib.start_service

def setup_logging(debug=True):
  '''Initialize common logging framework. Everything is logged to central
     graylog server. Depending on setting messages of DEBUG or INFO and higher
     go to console.'''
  logger = multiprocessing.get_logger()
  logger.setLevel(logging.DEBUG)

  # Enable logging to console
  console = logging.StreamHandler()
  if not debug:
    console.setLevel(logging.INFO)
  logger.addHandler(console)

  # Enable logging to graylog
  enable_graylog()
 
if __name__ == '__main__':
  # override default stomp host
  from workflows.transport.stomp_transport import StompTransport
  StompTransport.defaults['--stomp-host'] = 'ws154.diamond.ac.uk'

  # initialize logging
  setup_logging(debug=True)
  logger = logging.getLogger('dlstbx.service')

  logger.debug('Launching dlstbx.service with ' + str(sys.argv[1:]))

  dlstbx = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
  workflows.load_plugins([os.path.join(dlstbx, 'services')])
  workflows.contrib.start_service.run(sys.argv[1:],
                                      program_name='dlstbx.service',
                                      version=dlstbx_version())
