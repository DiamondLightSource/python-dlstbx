#
# dlstbx.graylog
#   tail -f equivalent for graylog messages
#

from __future__ import absolute_import, division
import ConfigParser
from dlstbx.util.colorstreamhandler import ColorStreamHandler
import json
import logging
import os
import sys
import time
import urllib2

class GraylogAPI():
  last_seen_message = None
  last_seen_timestamp = None

  def __init__(self, configfile):
    cfgparser = ConfigParser.ConfigParser(allow_no_value=True)
    if not cfgparser.read(configfile):
      raise WorkflowsError('Could not read from configuration file %s' % filename)
    self.url = cfgparser.get('graylog', 'url')
    if not self.url.endswith('/'):
      self.url += '/'
    self.username = cfgparser.get('graylog', 'username')
    self.password = cfgparser.get('graylog', 'password')
    self.stream = cfgparser.get('graylog', 'stream')

  def _get(self, url):
    complete_url = self.url + url
    password_manager = urllib2.HTTPPasswordMgrWithDefaultRealm()
    password_manager.add_password(None, complete_url, self.username, self.password)
    auth_manager = urllib2.HTTPBasicAuthHandler(password_manager)
    opener = urllib2.build_opener(auth_manager)
    urllib2.install_opener(opener)

    req = urllib2.Request(complete_url, headers={"Accept": "application/json"})
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

  def get_messages(self):
    if self.last_seen_timestamp:
      update = self.absolute_update()
    else:
      update = self.relative_update()
    if not update['success']:
      return
    update = update['parsed']
    messages = [ x.get('message', {}) for x in update.get('messages', []) ]
    if self.last_seen_message:
      message_ids = [ m['_id'] for m in messages ]
      try:
        seen_marker = message_ids.index(self.last_seen_message)
        messages = messages[seen_marker + 1:]
      except ValueError:
        pass # last seen message not in selection
    if messages:
      self.last_seen_message = messages[-1]['_id']
      self.last_seen_timestamp = messages[-1]['timestamp']
    return messages

  def relative_update(self, time=600):
    return self._get("search/universal/relative?"
                     "query=*&"
                     "range={time}&"
                     "filter=streams%3A{stream}&"
                     "sort=timestamp%3Aasc"
                     .format(time=time, stream=self.stream)
        )

  def absolute_update(self, from_time=None):
    if not from_time:
      from_time = self.last_seen_timestamp
    from_time = from_time.replace(':', '%3A')
    return self._get("search/universal/absolute?"
                     "query=*&"
                     "from={from_time}&"
                     "to=2031-01-01%2012%3A00%3A00&"
                     "filter=streams%3A{stream}&"
                     "sort=timestamp%3Aasc"
                     .format(from_time=from_time, stream=self.stream)
        )

if __name__ == '__main__':
  g = GraylogAPI('/dls_sw/apps/zocalo/secrets/credentials-log.cfg')
  import pprint
  while True:
    for message in g.get_messages():
      print ""
      print time.time()
      pprint.pprint(message)
    print ""
    print time.time()
    time.sleep(1)
