#
# dlstbx.recipe
#   Propose recipes to be run on $DCID
#

from __future__ import division

import json
import sys
from optparse import SUPPRESS_HELP, OptionParser

# Example: dlstbx.recipe 527189

if __name__ == '__main__':
  if len(sys.argv) != 2:
    raise RuntimeError, '%s $DCID' % sys.argv[0]

  dcid = int(sys.argv[1])

  from dlstbx.ispybtbx import ispyb_filter

  message = { }
  parameters = {'ispyb_dcid':dcid}

  message, parameters = ispyb_filter(message, parameters)

  print ' '.join(message['default_recipe'])
