# Returns the parameter dictionary that is available in recipes for a
# given data processing ID or reprocessing ID.

# Examples:
#
# dlstbx.find_in_ispyb 1956161
#   show available dictionary for DCID
#
# dlstbx.find_in_ispyb -p 95
#   show available dictionary for reprocessing ID

from __future__ import absolute_import, division

import pprint
import sys
from optparse import SUPPRESS_HELP, OptionParser

from dlstbx.ispybtbx import ispyb_filter

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.find_in_ispyb [options] dcid")
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)

  parser.add_option("-p", "--reprocessing", dest="reprocess",
      action="store_true", default=False,
      help="Means a reprocessing ID is given rather than a data collection ID")
  (options, args) = parser.parse_args(sys.argv[1:])

  for arg in args:
    if int(arg) > 0:
      parameters = {}
      if options.reprocess:
        parameters['ispyb_process'] = int(arg)
      else:
        parameters['ispyb_dcid'] = int(arg)
      message, parameters = ispyb_filter({}, parameters)
      pprint.pprint(parameters)

    else:
      # Not sure what this is.
      path = arg
      if not path.endswith('/'):
        path += '/'
      i = ispybtbx()
      dc_ids = i.find_dc_id(path)
      print path
      for dc_id in dc_ids:
        print dc_id
