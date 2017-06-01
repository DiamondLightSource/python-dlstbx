#
# dlstbx.submit_ispyb
#   Write information to ISPyB
#

from __future__ import division
from optparse import OptionParser, SUPPRESS_HELP
import sys
import workflows
from workflows.transport.stomp_transport import StompTransport

# Example: dlstbx.submit_ispyb --xia2 1515302
#     for https://ispyb.diamond.ac.uk/dc/visit/cm16788-1/id/1515302

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.submit_ispyb [options] [dcid]")

  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("--json", dest="json", action="append", default=[],
      help="Send pre-formatted JSON file.")
  parser.add_option("--xia2", dest="xia2", action="store_true", default=False,
      help="Read xia2.json/txt in current directory.")
  parser.add_option("--test", action="store_true", dest="test",
      help="Run in ActiveMQ testing (zocdev) namespace")

  parser.add_option("--replace-zocalo-tmpdir", dest="zocalo_tmp_dir_mode", action="store_true", default=False,
      help="Replace /tmp/zocalo/ in directory references with /processed/.")

  # override default stomp host
  default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'
  if '--test' in sys.argv:
    default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg'
  StompTransport.load_configuration_file(default_configuration)

  StompTransport.add_command_line_options(parser)
  (options, args) = parser.parse_args(sys.argv[1:])

  if not options.xia2 and not options.json and not args:
    parser.print_help()
    sys.exit(0)

  if not options.xia2 and not options.json:
    print "You need to specify something to be sent to ISPyB."
    sys.exit(1)

  messages = []

  for jsonfile in options.json:
    print "Loading", jsonfile
    with open(jsonfile, 'r') as fh:
      messages.append(json.load(fh))

  if options.xia2:
    print "Reading xia2 results"
    from xia2.command_line.ispyb_json import ispyb_object
    messages.append(ispyb_object())

  dcid_present = all( \
      all( container.get('AutoProcIntegration', {}).get('dataCollectionId') for container in \
           message.get('AutoProcScalingContainer', {}).get('AutoProcIntegrationContainer', []) ) \
      for message in messages )

  def recursive_replace(thing, old, new):
    '''Recursive string replacement in data structures.'''

    def _recursive_apply(item):
      '''Internal recursive helper function.'''
      if isinstance(item, basestring):
        return item.replace(old, new)
      if isinstance(item, dict):
        return { _recursive_apply(key): _recursive_apply(value) for
                 key, value in item.items() }
      if isinstance(item, tuple):
        return tuple(_recursive_apply(list(item)))
      if isinstance(item, list):
        return [ _recursive_apply(x) for x in item ]
      return item

    return _recursive_apply(thing)

  if options.zocalo_tmp_dir_mode:
    print "Replacing temporary zocalo paths with correct destination paths"
    messages = recursive_replace(messages, '/tmp/zocalo/', '/processed/')

  if not args:
    print "No data collection ID specified."
    sys.exit(1)

  if len(args) > 1:
    print "Only a single data collection ID can be specified."
    sys.exit(1)

  if len(args) == 1:
    dcid = int(args[0])
    assert dcid > 0, "Invalid data collection ID given."
    print "Writing to data collection ID", dcid
    for message in messages:
      for container in message['AutoProcScalingContainer']['AutoProcIntegrationContainer']:
        container['AutoProcIntegration']['dataCollectionId'] = dcid

  stomp = StompTransport()
  stomp.connect()

  for message in messages:
    print "Sending", message
    stomp.send(
      'ispyb',
      message
    )

  print "\nDone."
