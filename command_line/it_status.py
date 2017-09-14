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
  usage='it.status [options] [issue | issuegroup.]*'
)
parser.add_option("-?", action="help", help=SUPPRESS_HELP)
parser.add_option("-v", action="count", dest="verbosity",
    default=0, help="Increase verbosity level (up to 3x)")
parser.add_option("-q", action="count", dest="quiet",
    default=0, help="Be more quiet (up to 2x)")

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

prune = OptionGroup(parser, 'to clean up the database')
prune.add_option("--prune", dest="prune", action="store_true",
    default=False, help="Remove reports older than 24 hours")

(options, args) = parser.parse_args()

error_exists = False

def store_status():
  db = dlstbx.profiling.database()
  db.set_infrastructure_status(
    source=options.source,
    level=options.level,
    message=options.message,
    url=options.URL)

def prune_database():
  dlstbx.profiling.database().prune()
  print "Database successfully pruned"

def display_status(issues):
  global error_exists
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
  if issues:
    prefixes = filter(lambda x: x.endswith('.'), issues)
    status = filter(lambda x: x['Source'] in issues or any(map(lambda y: x['Source'].startswith(y), prefixes)), status)

  for group, colour in (('Error', logging.ERROR), \
                        ('Warning', logging.WARNING), \
                        ('Information', logging.INFO)):
    select = filter(lambda s: s['Group'] == group, status)
    if select:
      resetcolor()
      setcolor(colour)
      if not options.quiet:
        if options.verbosity > 0 or group != 'Information':
          setbold()
          print "\n%d %s message%s:" % (len(select), group, '' if len(select) == 1 else 's')
        else:
          print "\n%d %s message%s (omitted)" % (len(select), group, '' if len(select) == 1 else 's')
      if group == 'Information' and (options.quiet or options.verbosity == 0):
        continue
      if group == 'Error':
        error_exists = True
      base_indent = '' if options.quiet else '  '
      for s in select:
        resetcolor()
        setcolor(colour)
        age = (datetime.datetime.now() - s['Timestamp']).seconds
        if age < 30:
          age = "just now"
        elif age < 90:
          age = "%d sec ago" % age
        elif age < 90 * 60:
          age = "%d min ago" % round(age / 60)
        else:
          age = "%.1f hrs ago" % (age / 60 / 60)
        if s['Level'] > 0:
          setbold()
        print base_indent + s['Source'] + ":",
        resetcolor()
        setcolor(colour)
        print " %s (%s)" % (s['Message'], age)
        indent = base_indent + (len(s['Source']) + 2) * ' '
        if s['MessageBody'] and not options.quiet and (group != 'Information' or options.verbosity > 2):
          print indent + s['MessageBody'].replace('\n', '\n' + indent)
        if s['URL'] and (group != 'Information' or options.verbosity > 1) and not (options.quiet > 1):
          print indent + s['URL']
  resetcolor()

if options.prune:
  prune_database()
elif options.source:
  store_status()
else:
  display_status(args)

if error_exists:
  sys.exit(1)
