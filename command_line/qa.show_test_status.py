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

def run(options, datasets):
  dbfile = options.database
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
      if t['retired']:
        color()
        color('grey')
      else:
        color()
      if t['lastseen'] is None:
        print
      else:
        print " (%s ago%s)" % (units.readable_time(epoch - t['lastseen']), ", retired" if t['retired'] else '')
      color()

if __name__ == '__main__':
  parser = OptionParser("usage: %prog [options] [module [module [..]]]")
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-g", "--group", action="store_true", dest="group", help="group dataset tests together")
  parser.add_option("-a", "--all", action="store_true", dest="show_all", help="show all tests, including retired tests")
  parser.add_option("-d", "--database", dest="database", metavar="DBFILE", help="location of the quality-assurance database file", default="/dls/mx-scratch/mgerstel/qa/qa.db")
  run(*parser.parse_args())
