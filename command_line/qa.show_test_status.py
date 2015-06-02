#!/bin/env python

from datetime import datetime
import dlstbx.qa.database as db
from dlstbx.qa.term import color
import dlstbx.qa.units as units
from optparse import OptionParser, SUPPRESS_HELP
import os.path
import sys

def run(dbfile):
  if not os.path.exists(dbfile):
    print "Database file %s not found" % dbfile
    return
  sql = db.DB(dbfile)

  epoch = (datetime.now() - datetime(1970, 1, 1)).total_seconds()
  lastdataset = None
  for t in sql.get_tests(order_by_name=True):
    if lastdataset != t['dataset']:
      if lastdataset != None:
        print
      lastdataset = t['dataset']
      color('bright', 'white')
      print ' %s' % t['dataset']
      print '=' * (2 + len(t['dataset']))
      color()

    if t['success'] and not t['skipped']:
      color('green')
      print " [ OK ]",
    elif t['success']:
      color('yellow')
      print " [SKIP]",
    else:
      color('red')
      print " [FAIL]",
    print "%-30s" % t['test'],
    color()
    print " (%s ago)" % units.readable_time(epoch - t['lastseen'])

if __name__ == '__main__':
  parser = OptionParser("usage: %prog database.db [options] [module [module [..]]]")
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
#  parser.add_option("-v", "--verbose", action="store_true", dest="verbose", help="produce more output")
#  parser.add_option("-p", "--path", dest="path", metavar="PATH", help="Location of the quality-assurance directory structure (containing subdirectories /work /logs /archive)", default=".")
#  parser.add_option("-l", "--list", action="store_true", dest="list", help="list all available tests")
#  parser.add_option("-a", "--auto", action="store_true", dest="auto", help="automatically select and run one test")
  (options, args) = parser.parse_args()
  if len(args) < 1:
    parser.error('Location of the database file must be specified')

#  print "Options:  ", options
#  print "Arguments:", args

  run(args[0])
