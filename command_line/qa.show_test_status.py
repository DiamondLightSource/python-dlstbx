#!/bin/env python

from datetime import datetime
import dlstbx.qa.database as db
from dlstbx.qa.term import color
import dlstbx.qa.units as units
import os.path
import sys

def run(params):
  if len(params) < 1:
    print "usage: python dlstbx.qa.show_test_status.py database.db"
    return
  dbfile = params[0]
  if not os.path.exists(dbfile):
    print "Database file %s not found" % dbfile
    return
  sql = db.DB(dbfile)

  epoch = (datetime.now() - datetime(1970, 1, 1)).total_seconds()
  lastdataset = None
  for t in sql.get_tests(order_by_name=True):
    if lastdataset != t['dataset']:
      lastdataset = t['dataset']
      color('bright', 'white')
      print ' %s' % t['dataset']
      print '=' * (2 + len(t['dataset']))
      color()

    if t['success']:
      color('green')
      print " [ OK ]",
    else:
      color('red')
      print " [FAIL]",
    print "%-30s" % t['test'],
    color()
    print " (%s ago)" % units.readable_time(epoch - t['lastseen'])

if __name__ == '__main__':
  run(sys.argv[1:])
