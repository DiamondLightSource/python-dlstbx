# LIBTBX_SET_DISPATCHER_NAME ispyb.rpc

from __future__ import absolute_import, division, print_function

import json
import sys
from optparse import SUPPRESS_HELP, OptionParser
from workflows.transport.stomp_transport import StompTransport

# Example STDIN to create a AutoProcProgramAttachment entry:
#
# {
#   "parameters": { "ispyb_command": "add_program_attachment" },
#   "content": { "programid": 67755667,
#                "filename": "xia2.error",
#                "filepath": "/full/path/to/an/existing/file",
#                "filetype": "log"
#              }
# }
#

if __name__ == '__main__':
  parser = OptionParser(usage="ispyb.rpc [options]")
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("--test", action="store_true", dest="test", help="Run in ActiveMQ testing (zocdev) namespace")
  default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'
  if '--test' in sys.argv:
    default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-testing.cfg'
  # override default stomp host
  StompTransport.load_configuration_file(default_configuration)
  StompTransport.add_command_line_options(parser)
  (options, args) = parser.parse_args(sys.argv[1:])

  if sys.stdin.isatty():
    print("ispyb.rpc forwards a JSON formatted remote procedure call to the ISPyB connector service.")
    print("The JSON formatted dictionary must be passed via STDIN.")
    sys.exit(1)

  # Write results to ISPyB using zocalo
  message = json.load(sys.stdin)
# {
#     'parameters': { 'ispyb_command': 'store_per_image_analysis_results',
#                     'dcid': kwargs['dcid'],
#                     'notify-gda': beamline,
#                   },
#     'content': response
# }

  stomp = StompTransport()
  stomp.connect()
  stomp.send('ispyb_connector', message)
  stomp.disconnect()
