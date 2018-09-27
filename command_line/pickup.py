from __future__ import absolute_import, division, print_function

import json
import os
import sys
import time

files = os.listdir('/dls_sw/apps/zocalo/dropfiles')

print("Found %d files" % len(files))

if not files:
  sys.exit()

print("Connecting to stomp...")
from workflows.transport.stomp_transport import StompTransport
default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'
StompTransport.load_configuration_file(default_configuration)
stomp = StompTransport()
stomp.connect()

print("Waiting $NFS seconds")
time.sleep(60)

for f in files:
  print("Sending " + f)

  with open(os.path.join('/dls_sw/apps/zocalo/dropfiles', f)) as fh:
    message = json.load(fh)
  stomp.send('processing_recipe', message)
  os.remove(os.path.join('/dls_sw/apps/zocalo/dropfiles', f))
  print("Done")
  time.sleep(1)

