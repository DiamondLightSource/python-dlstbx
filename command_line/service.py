#
# dlstbx.service
#   Starts a workflow service
#

from __future__ import division
from dlstbx.util.version import dlstbx_version
import os.path
import logging
import sys
import workflows
import workflows.contrib.start_service

if __name__ == '__main__':
  # override default stomp host
  from workflows.transport.stomp_transport import StompTransport
  StompTransport.defaults['--stomp-host'] = 'ws154.diamond.ac.uk'

  logger = logging.getLogger('dlstbx')
  logger.setLevel(logging.DEBUG)
  fh = logging.FileHandler('dlstbx.services.log')
  fh.setLevel(logging.DEBUG)
  ch = logging.StreamHandler()
  ch.setLevel(logging.INFO)
  # create formatter and add it to the handlers
  fhformatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  chformatter = logging.Formatter('%(message)s')
  fh.setFormatter(fhformatter)
  ch.setFormatter(chformatter)
  logger.addHandler(fh)
  logger.addHandler(ch)
  # log dials output to file
  dials_logger = logging.getLogger('dials')
  dials_logger.addHandler(fh)
  dials_logger.setLevel(logging.INFO)
  logger.info(str(sys.argv))

  dlstbx = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
  workflows.load_plugins([os.path.join(dlstbx, 'services')])
  workflows.contrib.start_service.run(sys.argv[1:],
                                      program_name='dlstbx.service',
                                      version=dlstbx_version())
