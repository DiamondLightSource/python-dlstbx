#
# dlstbx.go
#   Process a datacollection
#

from __future__ import division
from optparse import OptionParser, SUPPRESS_HELP
import sys
import workflows
import workflows.contrib.start_service
from workflows.transport.stomp_transport import StompTransport

# Example: dlstbx.go -r example-xia2 527189

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.go [options] dcid [dcid ...]")

  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-r", "--recipe", dest="recipe", metavar="RCP",
      action="append", default=[],
      help="Name of a recipe to run. Can be used multiple times.")

  StompTransport.defaults['--stomp-host'] = 'cs04r-sc-vserv-128'
  StompTransport.defaults['--stomp-prfx'] = 'zocdev'
  StompTransport.add_command_line_options(parser)

  (options, args) = parser.parse_args(sys.argv[1:])

  if not args:
    print "No data collection IDs specified."
    sys.exit(0)

  if len(args) > 1:
    print "Currently only a single data collection ID can be specified."
    sys.exit(0)

  dcid = int(args[0])
  assert dcid > 0, "Invalid data collection ID given."

  print "Running recipes", options.recipe
  print "for data collection", dcid


  stomp = StompTransport()
  stomp.connect()
  stomp.send(
    'processing_recipe',
    {
      'recipes': options.recipe,
      'parameters': { 'ispyb_dcid': dcid }
    }
  )

  print "\nSubmitted."
