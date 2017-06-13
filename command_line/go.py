#
# dlstbx.go
#   Process a datacollection
#

from __future__ import division
from optparse import OptionParser, SUPPRESS_HELP
import json
import sys
import workflows
from workflows.transport.stomp_transport import StompTransport

# Example: dlstbx.go -r example-xia2 527189

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.go [options] dcid [dcid ...]")

  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-r", "--recipe", dest="recipe", metavar="RCP",
      action="append", default=[],
      help="Name of a recipe to run. Can be used multiple times. Recipe names correspond to filenames (excluding .json) in /dls_sw/apps/zocalo/live/recipes")
  parser.add_option("-a", "--autoprocscalingid", dest="autoprocscalingid", metavar="APSID",
      action="store", type="string", default=None,
      help="An auto processing scaling ID for downstream processing recipes.")
  parser.add_option("-f", "--file", dest="recipefile", metavar="FILE",
      action="store", type="string", default="",
      help="Run recipe contained in this file.")
  parser.add_option("-n", "--no-dcid", dest="nodcid",
      action="store_true", default=False,
      help="Trigger recipe without specifying a data collection ID")
  parser.add_option("-d", "--default", dest="default",
      action="store_true", default=False,
      help="Run default recipes for each given data collection ID")
  parser.add_option("--not", dest="disable",
      action="append", default=[],
      help="Do not run this recipe. Only evaluated when --default is used")

  parser.add_option("--test", action="store_true", dest="test", help="Run in ActiveMQ testing (zocdev) namespace")
  default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'
  if '--test' in sys.argv:
    default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg'

  # override default stomp host
  try:
    StompTransport.load_configuration_file(default_configuration)
  except workflows.WorkflowsError, e:
    raise

  StompTransport.add_command_line_options(parser)
  (options, args) = parser.parse_args(sys.argv[1:])
  stomp = StompTransport()

  message = { 'recipes': options.recipe,
              'parameters': {},
            }

  if not options.recipe and not options.recipefile and not (options.default and not options.nodcid):
    print "No recipes specified."
    sys.exit(1)

  if options.recipefile:
    with open(options.recipefile, 'r') as fh:
      message['custom_recipe'] = json.load(fh)

  if options.nodcid:
    if options.recipe:
      print "Running recipes", options.recipe
    if options.recipefile:
      print "Running recipe from file", options.recipefile
    print "without specified data collection."
    stomp.connect()
    stomp.send(
      'processing_recipe',
      message
    )
    print "\nSubmitted."
    sys.exit(0)

  if not args:
    print "No data collection IDs specified."
    sys.exit(1)

  if len(args) > 1:
    print "Currently only a single data collection ID can be specified."
    sys.exit(1)

  dcid = int(args[0])
  assert dcid > 0, "Invalid data collection ID given."

  if options.default:
    # Take a DCID. Find a list of default recipe names.
    from dlstbx.ispyb.ispyb import ispyb_filter
    default_recipes = ispyb_filter({ }, {'ispyb_dcid': dcid})[0]['default_recipe']

    # Merge with any manually specified recipes
    message['recipes'] = list( (set(message['recipes']) | set(default_recipes)) - set(options.disable) )

  if message['recipes']:
    print "Running recipes", message['recipes']

  if options.recipefile:
    print "Running recipe from file", options.recipefile

  if not message['recipes'] and not message.get('custom_recipe'):
    print "No recipes specified."
    sys.exit(1)
  print "for data collection", dcid
  message['parameters']['ispyb_dcid'] = dcid

  if options.autoprocscalingid:
    apsid = int(options.autoprocscalingid)
    assert apsid > 0, "Invalid auto processing scaling ID given."
    message['parameters']['ispyb_autoprocscalingid'] = apsid

  stomp.connect()
  stomp.send(
    'processing_recipe',
    message
  )
  print "\nSubmitted."
