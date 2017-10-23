#
# dlstbx.graylog
#   'tail' equivalent for graylog messages
#

from __future__ import absolute_import, division, print_function

import base64
import json
import string
import sys
import time
import urllib2
from optparse import SUPPRESS_HELP, OptionParser

from dlstbx.util.colorstreamhandler import ColorStreamHandler
from dlstbx.util.graylog import GraylogAPI

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

def format_message(verbosity):
  class SafeDict(dict):
    '''A dictionary that returns empty strings for undefined keys.'''
    @staticmethod
    def __missing__(key):
      '''Return an empty string.'''
      return ''

  def format_string(fstring, message):
    return string.Formatter().vformat(fstring, (), SafeDict(message))

  def format_default(message):
    return log_levels[message['level']]['color'] + \
           format_string( \
             "{timestamp} {facility}\n"
             "                         {full_message}\n",
             message
           ) + ColorStreamHandler.DEFAULT

  def format_source(message):
    return log_levels[message['level']]['color'] + \
           format_string( \
             "{timestamp} {facility} {source} - {workflows_service}\n"
             "                         {full_message}\n",
             message
           ) + ColorStreamHandler.DEFAULT

  def format_verbose(message):
    return format_string( \
             "{timestamp} {facility} {source} - {workflows_service}\n"
             "{file}:{line} ({function})\n"
             + log_levels[message['level']]['color'] +
             "{full_message}\n\n",
             message
           ) + ColorStreamHandler.DEFAULT

  def format_xverbose(message):
    return format_string( \
             "{timestamp} {facility} {source} - {workflows_service}\n"
             "workflows {workflows_workflows}  {workflows_dlstbx}  Status:{workflows_statustext}  {process_name}:{thread_name}\n"
             "{file}:{line} ({function})\n"
             + log_levels[message['level']]['color'] +
             "{full_message}\n\n",
             message
           ) + ColorStreamHandler.DEFAULT

  def format_raw(message):
    return message['timestamp'] + "\n" + log_levels[message['level']]['color'] + \
           "\n".join("%23s: %s" % (key, str(message[key]).replace("\n", "\n" + " " * 23 + "| ")) for key in sorted(message) if not key.startswith('gl2_')) + \
           "\n\n" + ColorStreamHandler.DEFAULT

  formats = [ format_default, format_source, format_verbose, format_xverbose, format_raw ]
  try:
    return formats[verbosity]
  except IndexError:
    return formats[-1]

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.graylog [options]")
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-f", "--follow", dest="follow", default=False, action="store_true",
                    help="Keep showing log messages as they come in.")
  parser.add_option("--level", dest="level", default="info",
                    help="Show messages with this loglevel and higher. Valid options: alert, critical, error, warning, notice, info, debug")
  parser.add_option("--time", dest="time", default=600, type="int",
                    help="Start showing messages from this many seconds back in time.")
  parser.add_option("-v", "--verbose", dest="verbose", default=0, action="count",
                    help="Show more detail (can be specified multiple times)")
  (options, args) = parser.parse_args(sys.argv[1:])

  try:
    level = [ 'a', 'c', 'e', 'w', 'n', 'i', 'd' ].index(options.level.lower()[0]) + 1
  except ValueError:
    print("Invalid loglevel specified.")
    sys.exit(1)

  g = GraylogAPI('/dls_sw/apps/zocalo/secrets/credentials-log.cfg')
  g.level = level
  format = format_message(options.verbose)
  if options.follow:
    try:
      while True:
        for message in g.get_messages(time=options.time):
          sys.stdout.write(format(message))
        sys.stdout.flush()
        time.sleep(0.7)
    except KeyboardInterrupt:
      print()
  else:
    for message in g.get_all_messages(time=options.time):
      sys.stdout.write(format(message))
