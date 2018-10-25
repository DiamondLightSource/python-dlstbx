from __future__ import absolute_import, division, print_function

import json
import os
import sys
import time

from workflows.transport.stomp_transport import StompTransport

dropdir = '/dls_sw/apps/zocalo/dropfiles'
files = os.listdir(dropdir)

print("Found %d files" % len(files))

if not files:
  sys.exit()

print("Connecting to stomp...")
default_configuration = '/dls_sw/apps/zocalo/secrets/credentials-live.cfg'
StompTransport.load_configuration_file(default_configuration)
stomp = StompTransport()
stomp.connect()

file_info = {
    f: {
        'filename': os.path.join(dropdir, f),
    } for f in files }
hosts = {}

print("Waiting $NFS seconds")
time.sleep(60)

recipe_priorities = {
  'per-image-analysis-dozor': -30,
  'xray-centering': -10,
  'per-image-analysis-gridscan': -10,
  'per-image-analysis-rotation': -5,
  'archive-cbf': 3,
  'most-recent-for-host': 30,
}

for f in file_info.values():
  with open(f['filename']) as fh:
    data = json.load(fh)
    f['message'] = data['message']
    f['headers'] = data['headers']
  f['originating-host'] = f['headers'].get('dlstbx.go.host')
  f['recipes'] = ','.join(f['message'].get('recipes', []))
  f['priority'] = sum(map(lambda r: recipe_priorities.get(r, 0), f['message'].get('recipes', [])))
  f['last-touch'] = os.path.getmtime(f['filename'])
  if hosts.get(f['originating-host'], {}).get('last-touch', 0) < f['last-touch']:
    hosts[f['originating-host']] = f

for f in hosts.values():
  f['priority'] += recipe_priorities['most-recent-for-host']

count = 0
file_count = len(file_info)
for f in sorted(file_info, key=lambda f: file_info[f]['priority'], reverse=True):
  print("Sending {f} from host {finfo[originating-host]} with recipes {finfo[recipes]}".format(f=f, finfo=file_info[f]))
  stomp.send('processing_recipe', file_info[f]['message'], headers=file_info[f]['headers'])
  os.remove(file_info[f]['filename'])
  count = count + 1
  print("Done ({count} of {total})".format(count=count, total=file_count))
  time.sleep(2)
