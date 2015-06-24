#!/bin/env python

from optparse import OptionParser, SUPPRESS_HELP

def merge_all(path):
  import os
  import xml.etree.cElementTree as et

  merged = et.Element('testsuites')
  for f in os.listdir(path):
    if f.endswith(".xml"):
      log = et.parse(os.path.join(path, f))
      suites = log.getroot()
      for n in suites.getchildren():
        merged.append(n)
  return et.tostring(merged)

if __name__ == '__main__':
  parser = OptionParser(
    usage="usage: %prog [directory]",
    description="merges all junit .xml result files from one directory")
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  (options, args) = parser.parse_args()
  if len(args) >= 1:
    path = args[0]
  else:
    path = '.'
  print merge_all(path)
