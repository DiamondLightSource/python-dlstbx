#
# dlstbx.go
#   Process a datacollection
#

from __future__ import absolute_import, division, print_function

import json
import os
import sys
from optparse import SUPPRESS_HELP, OptionParser
from pprint import pprint

import workflows
from workflows.transport.stomp_transport import StompTransport

# Example: dlstbx.go -r example-xia2 527189

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.go [options] dcid",
                        description="Triggers processing of a standard " \
    "recipe, of an arbitrary recipe from a local file, of default recipes " \
    "for a data collection ID, or of an entry in the ISPyB processing table.")

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
  parser.add_option("-p", "--reprocessing", dest="reprocess",
      action="store_true", default=False,
      help="Means a reprocessing ID is given rather than a data collection ID")
  parser.add_option("-v", "--verbose", dest="verbose",
      action="store_true", default=False,
      help="Show raw message before sending")

  parser.add_option("--test", action="store_true", dest="test", default=False,
      help="Run in ActiveMQ testing (zocdev) namespace")
  default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'
  allow_stomp_fallback = True # and not any('stomp' in s.lower() for s in sys.argv)
  if '--test' in sys.argv:
    default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg'
    allow_stomp_fallback = False
  # override default stomp host
  try:
    StompTransport.load_configuration_file(default_configuration)
  except workflows.Error as e:
    print("Error: %s\n" % str(e))
    allow_stomp_fallback = False

  StompTransport.add_command_line_options(parser)
  (options, args) = parser.parse_args(sys.argv[1:])

  def send_to_stomp_or_defer(message):
    if options.verbose:
      pprint(message)
    try:
      stomp = StompTransport()
      stomp.connect()
      stomp.send('processing_recipe', message)
    except (KeyboardInterrupt, SyntaxError, AssertionError, AttributeError, ImportError, TypeError, ValueError):
      raise
    except Exception as e:
      if not allow_stomp_fallback:
        raise
      print("\n\n")
      import traceback
      traceback.print_exc()
      print("\n\nAttempting to store message in fallback location")
      message_serialized = json.dumps(message, indent=2) + "\n"
      import uuid
      fallback = os.path.join('/dls_sw/apps/zocalo/dropfiles', str(uuid.uuid4()))
      with open(fallback, 'w') as fh:
        fh.write(message_serialized)
      print("Message successfully stored in %s" % fallback)

  message = { 'recipes': options.recipe,
              'parameters': {},
            }

  if not options.recipe and not options.recipefile and not (options.default and not options.nodcid) and not options.reprocess:
    print("No recipes specified.")
    sys.exit(1)

  if options.recipefile:
    with open(options.recipefile, 'r') as fh:
      message['custom_recipe'] = json.load(fh)

  if options.nodcid:
    if options.recipe:
      print("Running recipes", options.recipe)
    if options.recipefile:
      print("Running recipe from file", options.recipefile)
    print("without specified data collection.")
    send_to_stomp_or_defer(message)
    print("\nSubmitted.")
    sys.exit(0)

  if not args:
    print("No data collection IDs specified.")
    sys.exit(1)

  if len(args) > 1:
    print("Only a single data collection ID can be specified.")
    sys.exit(1)

  dcid = int(args[0])
  assert dcid > 0, "Invalid data collection ID given."

  if options.reprocess:
    # Given ID is a reprocessing ID. Nothing else needs to be specified.
    if options.recipe:
      print("Running recipes", options.recipe)
    message['parameters']['ispyb_process'] = dcid
    send_to_stomp_or_defer(message)
    print("\nReprocessing task submitted for ID %d." % dcid)
    sys.exit(0)

  if options.default:
    # Take a DCID. Find a list of default recipe names.
    from dlstbx.ispybtbx import ispyb_filter
    default_recipes = ispyb_filter({ }, {'ispyb_dcid': dcid})[0]['default_recipe']

    # Merge with any manually specified recipes
    message['recipes'] = list( (set(message['recipes']) | set(default_recipes)) - set(options.disable) )

  if message['recipes']:
    print("Running recipes", message['recipes'])

  if options.recipefile:
    print("Running recipe from file", options.recipefile)

  if not message['recipes'] and not message.get('custom_recipe'):
    print("No recipes specified.")
    sys.exit(1)
  print("for data collection", dcid)
  message['parameters']['ispyb_dcid'] = dcid

  if options.autoprocscalingid:
    apsid = int(options.autoprocscalingid)
    assert apsid > 0, "Invalid auto processing scaling ID given."
    message['parameters']['ispyb_autoprocscalingid'] = apsid

  send_to_stomp_or_defer(message)
  print("\nSubmitted.")
