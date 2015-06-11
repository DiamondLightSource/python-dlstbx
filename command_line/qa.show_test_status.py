#!/bin/env python

from datetime import datetime
import dlstbx.qa.database as db
from dlstbx.qa.term import color
import dlstbx.qa.units as units
from optparse import OptionParser, SUPPRESS_HELP
import os.path
import sys

def indicate_success(t):
  if t['lastseen'] is None:
    color('grey')
    print " [----]",
  elif t['success'] and not t['skipped']:
    color('green')
    print " [ OK ]",
  elif t['success']:
    color('bright', 'yellow')
    print " [SKIP]",
  else:
    color('red')
    print " [FAIL]",

def run(options, args):
  dbfile = args[0]
  datasets = args[1:]
  if not os.path.exists(dbfile):
    print "Database file %s not found" % dbfile
    return
  sql = db.DB(dbfile)

  epoch = (datetime.now() - datetime(1970, 1, 1)).total_seconds()
  lastdataset = None

  if datasets == []:
    testlist = sql.get_tests(order_by_name=True, group_by_dataset=options.group, ignore_retired=not options.show_all)
  else:
    testlist = [t for dataset in datasets for t in sql.get_tests(dataset=dataset, order_by_name=True, group_by_dataset=options.group, ignore_retired=not options.show_all)]

  for t in testlist:
    if lastdataset != t['dataset']:
      if options.group:
        indicate_success(t)
      else:
        if lastdataset != None:
          print
        lastdataset = t['dataset']
        color('bright', 'white')
        print '',
      print '%s' % t['dataset'],
      color()
      color('grey')
      print ' (%s)' % t['runpriority']
      if not options.group:
        color('bright', 'white')
        print '=' * (2 + len(t['dataset']))
      color()
    if not options.group:
      indicate_success(t)
      print "%-30s" % t['test'],
      color()
      if t['lastseen'] is None:
        print
      else:
        print " (%s ago)" % units.readable_time(epoch - t['lastseen'])

if __name__ == '__main__':
  parser = OptionParser("usage: %prog database.db [options] [module [module [..]]]")
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-g", "--group", action="store_true", dest="group", help="group dataset tests together")
  parser.add_option("-a", "--all", action="store_true", dest="show_all", help="show all tests, including retired tests")
#  parser.add_option("-p", "--path", dest="path", metavar="PATH", help="Location of the quality-assurance directory structure (containing subdirectories /work /logs /archive)", default=".")
#  parser.add_option("-l", "--list", action="store_true", dest="list", help="list all available tests")
#  parser.add_option("-a", "--auto", action="store_true", dest="auto", help="automatically select and run one test")
  (options, args) = parser.parse_args()
  if len(args) < 1:
    parser.error('Location of the database file must be specified')

  run(options, args)
