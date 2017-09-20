#
# simple API to access Graylog messages
#
# Point to a configuration file to use it, eg:
#  g = GraylogAPI('/dls_sw/apps/zocalo/secrets/credentials-log.cfg')

from __future__ import absolute_import, division

import base64
import ConfigParser
import datetime
import json
import urllib2

class GraylogAPI():
  last_seen_message = None
  last_seen_timestamp = None

  def __init__(self, configfile):
    cfgparser = ConfigParser.ConfigParser(allow_no_value=True)
    self.level = 6 # INFO
    if not cfgparser.read(configfile):
      raise RuntimeError('Could not read from configuration file %s' % configfile)
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

  @staticmethod
  def epoch_to_graylog(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).isoformat().replace('T', ' ')

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
      messages = self.get_messages(time=time)
      for message in messages:
        yield message

  def absolute_histogram(self, from_time=None, level=None, level_op='%3C='):
    if not from_time:
      from_time = self.last_seen_timestamp
    from_time = from_time.replace(':', '%3A').replace(' ', '%20')
    if not level:
      level = self.level
    return self._get("search/universal/absolute/histogram?"
                     "query=level:{level_op}{level}&"
                     "interval=minute&"
                     "from={from_time}&"
                     "to=2031-01-01%2012%3A00%3A00&"
                     "filter=streams%3A{stream}"
                     .format(from_time=from_time, stream=self.stream,
                             level=level, level_op=level_op)
        )

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

  def gather_log_levels_histogram_since(self, from_timestamp):
    ts = self.epoch_to_graylog(from_timestamp)
    global_histdata = {}
    for level in (7, 6, 5, 4, 3, 2):
      hist = self.absolute_histogram(from_time=ts, level=level, level_op='' if level > 2 else '%3C=')
      assert hist['parsed'], 'Could not read histogram for level %d' % level
      for k, v in hist['parsed']['results'].items():
        k = int(k)
        if k not in global_histdata:
          global_histdata[k] = {}
        global_histdata[k][level] = v
    return global_histdata
