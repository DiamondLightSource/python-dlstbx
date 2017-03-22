#!/usr/bin/env python
#
# LIBTBX_SET_DISPATCHER_NAME it.status

from __future__ import absolute_import, division
import datetime
import dlstbx.profiling
from dlstbx.util.colorstreamhandler import ColorStreamHandler
import logging
from optparse import OptionGroup, OptionParser, SUPPRESS_HELP
import sys

parser = OptionParser(
  usage='it.status [options]'
)
parser.add_option("-?", action="help", help=SUPPRESS_HELP)
parser.add_option("-v", action="store_true", dest="verbose",
    default=False, help="Show information level messages")

report = OptionGroup(parser, 'to add a report to the database')
report.add_option("-s", "--source", dest="source", metavar="SRC",
    default=None, help="Add a report for this source to the database")
report.add_option("-l", "--level", dest="level", metavar="LVL",
    default=0, help="Warning level (0-9: OK, 11-19: Warn, 20+: Error)")
report.add_option("-m", "--message", dest="message", metavar="MSG",
    default=None, help="A tweet-long (<140 chars) status message")
report.add_option("-u", "--url", dest="URL",
    default=None, help="Optional link to more information")
parser.add_option_group(report)

(options, args) = parser.parse_args()

def store_status():
  db = dlstbx.profiling.database()
  db.set_infrastructure_status(
    source=options.source,
    level=options.level,
    message=options.message,
    url=options.URL)

def display_status():
  if hasattr(ColorStreamHandler, '_get_color'):
    def setbold():
      sys.stdout.write(ColorStreamHandler.BOLD)
    def setcolor(level):
      sys.stdout.write(getattr(ColorStreamHandler, '_get_color')(level))
    def resetcolor():
      sys.stdout.write(ColorStreamHandler.DEFAULT)
  else:
    setbold = lambda: None
    setcolor = lambda x: None
    resetcolor = lambda: None

  db = dlstbx.profiling.database()
  status = db.get_infrastructure_status()
  status = sorted(status, key=lambda s:-s['Level'])

  for group, colour in (('Error', logging.ERROR), \
                        ('Warning', logging.WARNING), \
                        ('Information', logging.INFO)):
    select = filter(lambda s: s['Group'] == group, status)
    if select:
      setcolor(colour)
      if options.verbose or group != 'Information':
        setbold()
        print "\n%d %s message%s:" % (len(select), group, '' if len(select) == 1 else 's')
      else:
        print "\n%d %s message%s (omitted)" % (len(select), group, '' if len(select) == 1 else 's')
        continue
      resetcolor()
      setcolor(colour)
      for s in select:
        age = (datetime.datetime.now() - s['Timestamp']).seconds
        if age < 30:
          age = "just now"
        elif age < 90:
          age = "%d seconds ago" % age
        else:
          age = "%d minutes ago" % round(age / 60)
        print "  %s: %s (%s)" % (s['Source'], s['Message'], age)
        indent = (len(s['Source']) + 4) * ' '
        if s['MessageBody']:
          print indent + s['MessageBody'].replace('\n', '\n' + indent)
        if s['URL']:
            print indent + s['URL']
  resetcolor()

if options.source:
  store_status()
else:
  display_status()
