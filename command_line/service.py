#
# dlstbx.service
#   Starts a workflow service
#

from __future__ import division
from dlstbx.util.version import dlstbx_version
import sys
import workflows.contrib.start_service

if __name__ == '__main__':
  workflows.contrib.start_service.run(sys.argv[1:],
                                      program_name='dlstbx.service',
                                      version=dlstbx_version())

