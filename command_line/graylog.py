#
# dlstbx.graylog
#   'tail' equivalent for graylog messages
#

from __future__ import absolute_import, division
import ConfigParser
from dlstbx.util.colorstreamhandler import ColorStreamHandler
import base64
import json
from optparse import OptionParser, SUPPRESS_HELP
import sys
import time
import urllib2

class GraylogAPI():
  last_seen_message = None
  last_seen_timestamp = None

  def __init__(self, configfile):
    cfgparser = ConfigParser.ConfigParser(allow_no_value=True)
    self.level = 6 # INFO
    if not cfgparser.read(configfile):
      raise RuntimeError('Could not read from configuration file %s' % filename)
    self.url = cfgparser.get('graylog', 'url')
    if not self.url.endswith('/'):
      self.url += '/'
    self.authstring = "Basic " \
                    + base64.b64encode(cfgparser.get('graylog', 'username')
                                                                 + ':' +
                                       cfgparser.get('graylog', 'password'))
    self.stream = cfgparser.get('graylog', 'stream')

  def _get(self, url):
    complete_url = self.url + url
    req = urllib2.Request(complete_url, headers={"Accept": "application/json"})
    req.add_header("Authorization", self.authstring)
    handler = urllib2.urlopen(req)

    returncode = handler.getcode()
    success = returncode == 200
    headers = { k: v for k, v in handler.headers.items() }
    body = handler.read()
    if success:
      parsed = json.loads(body)
    else:
      parsed = None

    return {
      "success": success,
      "returncode": returncode,
      "headers": headers,
      "body": body,
      "parsed": parsed
    }

  def get_messages(self, time=600):
    if self.last_seen_timestamp:
      update = self.absolute_update()
    else:
      update = self.relative_update(time=time)
      self.last_seen_timestamp = update['parsed']['to']
    if not update['success']:
      return
    update = update['parsed']
    messages = [ x.get('message', {}) for x in update.get('messages', []) ]
    if self.last_seen_message:
      message_ids = [ m['_id'] for m in messages ]
      try:
        seen_marker = message_ids.index(self.last_seen_message)
        messages = messages[seen_marker + 1:] # skip previously seen message and all preceeding
      except ValueError:
        pass # last seen message not in selection
    if messages:
      self.last_seen_message = messages[-1]['_id']
      self.last_seen_timestamp = messages[-1]['timestamp']
    return messages

  def get_all_messages(self, time=600):
    messages = True
    while messages:
      messages = g.get_messages(time=options.time)
      for message in messages:
        yield message

  def relative_update(self, time=600):
    return self._get("search/universal/relative?"
                     "query=level:%3C={level}&"
                     "range={time}&"
                     "filter=streams%3A{stream}&"
                     "sort=timestamp%3Aasc"
                     .format(time=time, stream=self.stream, level=self.level)
        )

  def absolute_update(self, from_time=None):
    if not from_time:
      from_time = self.last_seen_timestamp
    from_time = from_time.replace(':', '%3A')
    return self._get("search/universal/absolute?"
                     "query=level:%3C={level}&"
                     "from={from_time}&"
                     "to=2031-01-01%2012%3A00%3A00&"
                     "filter=streams%3A{stream}&"
                     "sort=timestamp%3Aasc"
                     .format(from_time=from_time, stream=self.stream, level=self.level)
        )

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

def format(message):
  return ( log_levels[message['level']]['color']
         + ("{timestamp} {source} {facility}\n" \
            "  {full_message}\n".format(**message))
         + ColorStreamHandler.DEFAULT)

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.graylog [options]")
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-f", "--follow", dest="follow", default=False, action="store_true",
                    help="Keep showing log messages as they come in.")
  parser.add_option("--level", dest="level", default="info",
                    help="Show messages with this loglevel and higher. Valid options: alert, critical, error, warning, notice, info, debug")
  parser.add_option("--time", dest="time", default=600, type="int",
                    help="Start showing messages from this many seconds back in time.")
  (options, args) = parser.parse_args(sys.argv[1:])

  try:
    level = [ 'a', 'c', 'e', 'w', 'n', 'i', 'd' ].index(options.level.lower()[0]) + 1
  except ValueError:
    print "Invalid loglevel specified."
    sys.exit(1)

  g = GraylogAPI('/dls_sw/apps/zocalo/secrets/credentials-log.cfg')
  g.level = level
  if options.follow:
    try:
      while True:
        for message in g.get_messages(time=options.time):
          sys.stdout.write(format(message))
        sys.stdout.flush()
        time.sleep(0.7)
    except KeyboardInterrupt:
      print
  else:
    for message in g.get_all_messages(time=options.time):
      sys.stdout.write(format(message))
