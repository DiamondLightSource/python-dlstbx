#!/usr/bin/env python
#
# LIBTBX_SET_DISPATCHER_NAME it.status

from __future__ import absolute_import, division
import datetime
import dlstbx.profiling
from dlstbx.util.colorstreamhandler import ColorStreamHandler
import logging
import sys

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
      setbold()
      print "\n%d %s message%s:" % (len(select), group, '' if len(select) == 1 else 's')
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

display_status()
