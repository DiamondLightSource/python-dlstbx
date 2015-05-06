# global imports for testcases

from result import Result
from comparators import *
from decorators import Data, Test
from testsuite import *
from units import *
from term import *
import os

_debug = False

def _archive_path(module):
  from datetime import datetime
  now = datetime.now()
  return os.path.join(_settings['archive'], "%04d" % now.year, "%02d" % now.month, module)

if __name__ == "__main__":
  import os
  import sys
  import decorators
  from optparse import OptionParser, SUPPRESS_HELP

  # ensure all created files are group writeable and publically readable
  os.umask((os.umask(0) | 075) - 075)

  parser = OptionParser("usage: %prog [options] [module [module [..]]]")
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
#  parser.add_option("-v", "--verbose", action="store_true", dest="verbose", help="produce more output")
#  parser.add_option("-q", "--quiet", action="store_true", dest="quiet", help="produce less output")
  parser.add_option("-p", "--path", dest="path", metavar="PATH", help="Location of the quality-assurance directory structure (containing subdirectories /work /logs /archive)", default=".")
  parser.add_option("-d", "--datapath", dest="datapath", metavar="PATH", help="Location of the data (default: /dls/mx-scratch/mgerstel/qa/data)", default="/dls/mx-scratch/mgerstel/qa/data")
  (options, args) = parser.parse_args()

  import loader

  if _debug:
    print "Options:  ", options
    print "Arguments:", args

  global _settings
  _settings = { 'basedir': os.path.abspath(options.path),
                'datadir': options.datapath }
  _settings['workdir'] = os.path.join(_settings['basedir'], 'work')
  _settings['logdir'] = os.path.join(_settings['basedir'], 'logs')
  _settings['archive'] = os.path.join(_settings['basedir'], 'archive')

  print "   Base directory:", _settings['basedir']
  print "   Data directory:", _settings['datadir']
  print "   Work directory:", _settings['workdir']
  print "    Log directory:", _settings['logdir']
  print "Archive directory:", _settings['archive']
  print

  if (len(args) == 0):
    print "Available tests:"
    loader.show_all_tests(_settings['datadir'])
    print "Specify tests on command line to run them"
  else:
    for t in args:
      results = loader.run_test_module(t,
         os.path.join(_settings['datadir'], t),
         os.path.join(_settings['workdir'], t),
         _archive_path(t))
      from junit_xml import TestSuite

      ts = TestSuite("dlstbx.qa.%s" % t, results)
      with open(os.path.join(_settings['logdir'], '%s.xml' % t), 'w') as f:
        TestSuite.to_file(f, [ts], prettyprint=False)
