# global imports for testcases

from result import Result
from comparators import *
from decorators import Data, Test
from testsuite import *
from units import *
from term import *

_debug = False

def works():
  # temporary function to check command line script
  return True

if __name__ == "__main__":
  import database
  import datetime
  now = datetime.datetime.now()
  import decorators
  import junit_xml
  import loader
  from optparse import OptionParser, SUPPRESS_HELP
  import os
  import sys

  # ensure all created files are group writeable and publically readable
  os.umask((os.umask(0) | 075) - 075)

  parser = OptionParser("usage: %prog [options] [module [module [..]]]")
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
#  parser.add_option("-v", "--verbose", action="store_true", dest="verbose", help="produce more output")
  parser.add_option("-p", "--path", dest="path", metavar="PATH", help="Location of the quality-assurance directory structure (containing subdirectories /work /logs /archive)", default=".")
  parser.add_option("-d", "--datapath", dest="datapath", metavar="PATH", help="Location of the data (default: /dls/mx-scratch/mgerstel/qa/data)", default="/dls/mx-scratch/mgerstel/qa/data")
  parser.add_option("-b", "--database", dest="database", metavar="FILE", help="Location of the sqlite3 qa database (default: ./qa.db relative to $path)", default="./qa.db")
  parser.add_option("-l", "--list", action="store_true", dest="list", help="list all available tests")
  parser.add_option("-a", "--auto", action="store_true", dest="auto", help="automatically select and run one test")
  (options, args) = parser.parse_args()

  if _debug:
    print "Options:  ", options
    print "Arguments:", args

  basedir = os.path.abspath(options.path)
  dbfile  = os.path.normpath(os.path.join(basedir, options.database))
  datadir = options.datapath
  workdir = os.path.join(basedir, 'work')
  logdir  = os.path.join(basedir, 'logs')
  archive = os.path.join(basedir, 'archive', "%04d" % now.year, "%02d" % now.month)

  print "   Base directory:", basedir
  print "         Database:", dbfile
  print "   Data directory:", datadir
  print "   Work directory:", workdir
  print "    Log directory:", logdir
  print "Archive directory:", archive

  with database.DB(dbfile) as db:
    loader = loader.Loader(db)
    if options.list:
      print "\nAvailable tests:"
      loader.show_all_tests(datadir)
    testlist = args
    if options.auto:
      modules_available = set(loader.list_all_modules())
      modules_status = db.get_tests(group_by_dataset=True)

      # auto-selection priority:
      #  - triggered test (don't yet know how)
      #  - tests that never ran, any order
      seen_modules = { t['dataset'] for t in modules_status }
      untested_modules = modules_available - seen_modules
      if len(untested_modules) > 0:
        testlist = [ untested_modules.pop() ]
      else:
      #  - tests that failed more than 24 hours ago, by age
      #  - oldest test
        print modules_available
        print modules_status
        testlist = [ "rhogdi2a4" ] #  TODO
    if (len(testlist) > 0):
      for t in testlist:
        results = loader.run_test_module(t,
          os.path.join(datadir, t),
          os.path.join(workdir, t),
          os.path.join(archive, t))

        ts = junit_xml.TestSuite("dlstbx.qa.%s" % t, results)
        with open(os.path.join(logdir, '%s.xml' % t), 'w') as f:
          junit_xml.TestSuite.to_file(f, [ts], prettyprint=False)
