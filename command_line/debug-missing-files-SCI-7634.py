#
# Debugging tool specifically for SCI-7634
# based on dlstbx.graylog
#

from __future__ import absolute_import, division, print_function

import ispyb
import json
import os
import re
import string
import sys
from optparse import SUPPRESS_HELP, OptionParser

from dlstbx.util.colorstreamhandler import ColorStreamHandler
from dlstbx.util.graylog import GraylogAPI

borken_DCIDs = {}

log_levels = {
    0: {'name': 'emerg', 'color': ColorStreamHandler.CRITICAL },
    1: {'name': 'alert', 'color': ColorStreamHandler.CRITICAL },
    2: {'name': 'crit', 'color': ColorStreamHandler.CRITICAL },
    3: {'name': 'err', 'color': ColorStreamHandler.ERROR },
    4: {'name': 'warning', 'color': ColorStreamHandler.WARNING },
    5: {'name': 'notice', 'color': ColorStreamHandler.INFO },
    6: {'name': 'info', 'color': ColorStreamHandler.INFO },
    7: {'name': 'debug', 'color': ColorStreamHandler.DEBUG },
  }

class SafeDict(dict):
  '''A dictionary that returns empty strings for undefined keys.'''
  def __missing__(self, key):
    '''Return an empty string.'''
    assert key != 'level'
    return { 'BOLD': ColorStreamHandler.BOLD,
             'DEFAULT': ColorStreamHandler.DEFAULT,
             'LEVEL': log_levels[self['level']]['color'],
           }.get(key, '')

def format_string(fstring, message):
  return string.Formatter().vformat(fstring, (), SafeDict(message))

def format_default(message):
  return format_string( \
           "{localtime:%Y-%m-%d %H:%M:%S}.{timestamp_msec}{LEVEL} {message}\n",
           message
         ) + ColorStreamHandler.DEFAULT

isp = ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')

def debug_message(message):
  m = re.search('\(([0-9]+) files found', message['message'])
  found = int(m.group(1))
  recipe_file = '/dls/tmp/zocalo/dispatcher/{l:%Y-%m}/{r:.2}/{r}'.format(l=message['localtime'], r=message['recipe_ID'])
  with open(recipe_file) as fh:
    json_data = None
    for line in fh:
      if line.startswith('Incoming message body:'):
        json_data = ''
      elif json_data is not None:
        json_data = json_data + line
        if line.startswith('}'):
          break
    assert json_data, 'No JSON data decoded'
  data = json.loads(json_data)
  dcid = data.get('parameters', {}).get('ispyb_dcid')
  dc = isp.get_data_collection(dcid)
  if dc.status == 'DataCollection Stopped':
    return
  if dcid in borken_DCIDs:
    return
  borken_DCIDs[dcid] = True
  sys.stdout.write(format_default(message))
  print("Recipe file   :", recipe_file)
  print("DCID          :", dcid)
  print("Files found   :", found)
  print("Files expected:", dc.image_count)
  print("Comment       :", data.get('parameters', {}).get('ispyb_dc_info', {}).get('comments'))
  print("Status        :", dc.status)
  print()

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.graylog [options]")
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("--time", dest="time", default=24 * 60 * 60, type="int",
                    help="Start showing messages from this many seconds back in time.")
  (options, args) = parser.parse_args(sys.argv[1:])

  g = GraylogAPI('/dls_sw/apps/zocalo/secrets/credentials-log.cfg')
  for message in g.get_all_messages(time=options.time, query='facility:dlstbx.services.filewatcher AND message:"timed out after" AND level:<6'):
    try:
      debug_message(message)
    except Exception as e:
      sys.stdout.write(format_default(message))
      print(str(e))
