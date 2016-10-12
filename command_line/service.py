#
# dlstbx.service
#   Starts a workflow service
#

from __future__ import division
from dlstbx.util.version import dlstbx_version
import os.path
import sys
import workflows
import workflows.contrib.start_service

if __name__ == '__main__':
  dlstbx = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
  workflows.load_plugins([os.path.join(dlstbx, 'services')])
  workflows.contrib.start_service.run(sys.argv[1:],
                                      program_name='dlstbx.service',
                                      version=dlstbx_version())

