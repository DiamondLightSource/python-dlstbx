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
  parser = OptionParser(usage="dlstbx.submit_ispyb [options] dcid")

  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("--json", dest="json", action="append", default=[],
      help="Send pre-formatted JSON file.")
  parser.add_option("--xia2", dest="xia2", action="store_true", default=False,
      help="Read xia2.json/txt in current directory.")

  # override default stomp host
  try:
    StompTransport.load_configuration_file(
      '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg')
  except workflows.WorkflowsError, e:
    raise

  StompTransport.add_command_line_options(parser)
  (options, args) = parser.parse_args(sys.argv[1:])

  if not options.xia2 and not options.json and not args:
    parser.print_help()
    sys.exit(0)

  if not args:
    print "No data collection ID specified."
    sys.exit(1)

  if len(args) > 1:
    print "Only a single data collection ID can be specified."
    sys.exit(1)

  dcid = int(args[0])
  assert dcid > 0, "Invalid data collection ID given."

  if not options.xia2 and not options.json:
    print "You need to specify something to be sent to ISPyB."
    sys.exit(1)

  stomp = StompTransport()
  stomp.connect()

  def send_message(message_object):
    print "Sending", message_object
    stomp.send(
      'transient.destination',
      message_object
    )

  for jsonfile in options.json:
    print "Sending", jsonfile
    with open(jsonfile, 'r') as fh:
      message = json.load(fh)
    send_message(message)

  if options.xia2:
    print "Reading xia2 results"
    from xia2.command_line.ispyb_json import ispyb_object
    message = ispyb_object()
    print "Sending xia2 results"
    send_message(message)

  print "\nDone."
