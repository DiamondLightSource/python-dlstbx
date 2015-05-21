#!/bin/env python

import dlstbx.qa.qa as qa

if __name__ == '__main__':
  print "xia2 qa:",
  if qa.works():
    print "ok"
  else:
    print "fail"
