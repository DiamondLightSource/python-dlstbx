#
# dlstbx.go
#   Process a datacollection
#

from __future__ import division
import workflows
import workflows.contrib.start_service

if __name__ == '__main__':
  from workflows.transport.stomp_transport import StompTransport
  StompTransport.defaults['--stomp-host'] = 'ws154.diamond.ac.uk'
  StompTransport.defaults['--stomp-prfx'] = 'pd'

  stomp = StompTransport()
  stomp.connect()

  dcid = 527189
  stomp.send(
    'processing_recipe',
    {
      'recipes': [ 'example-xia2' ],
      'parameters': { 'ispyb_dcid': dcid }
    }
  )
