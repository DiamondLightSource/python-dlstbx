from result import Result
from comparators import *
from decorators import Data, Test
from testsuite import *
from units import *
from term import *
import os

_loaded_modules = {}
_debug = False

def _archive_path(module):
  from datetime import datetime
  now = datetime.now()
  return os.path.join(_settings['archive'], "%04d" % now.year, "%02d" % now.month, module)

def _load_test_module(name):
  if name in _loaded_modules:
    return _loaded_modules[name]

  loader = { "name" : name }
  decorators.disableDecoratorFunctions()
  loader['module'] = tests.load_module(name)
  decorators.enableDecoratorFunctions()
  loader['Test()'] = decorators.getDiscoveredTestFunctions()
  loader['Data()'] = decorators.getDiscoveredDataFunctions()
  loader['runlog'] = {}
  loader['datadir'] = os.path.join(_settings['datadir'], name)
  loader['workdir'] = os.path.join(_settings['workdir'], name)
  loader['archivedir'] = _archive_path(name)
  loader['errors'] = [ "Test runs function %s() on import. Tests should not run functions on import." % n for n in decorators.disabledCalls() ]
  if not os.path.exists(loader['datadir']):
    loader['errors'].append( "Data directory %s is missing" % loader["datadir"] )

  loader['result'] = Result()
  for e in loader['errors']:
    loader['result'].log_error(e)
  loader['result'].set_name('__init__')

  _loaded_modules[name] = loader
  if _debug:
    print loader
  return loader

def _show_all_tests():
  for k, _ in tests.list_all_modules().iteritems():
    test = _load_test_module(k)
    testlist = ", ".join([name for (name, func, args, kwargs) in test["Test()"]])
    if test["errors"]:
      color('bright', 'red')
    print "  %25s : [ %s ]" % (k, testlist)
    if test["errors"]:
      print "  This test contains errors:"
      color('', 'red')
      for n in test["errors"]:
        print "    %s" % n
      print
      color()

def _run_test_function(module, func, xia2callRequired=False):
  import testsuite
  module['currentTest'] = func
  failure = None
  stacktrace = None

  testsuite.resetTestResults()
  testsuite.setModule(module.copy())

  try:
    func[1]()
  except Exception as e:
    e_type, e_value, e_traceback = sys.exc_info()
    import traceback
    stacktrace = ("".join(traceback.format_tb(e_traceback)[1:])).strip()
    failure = "Test resulted in error: %s" % e
  if xia2callRequired:
    testsuite.checkTestResults()
  testresults = testsuite.getTestOutput()
  testresults.set_name(func[0])

  if failure is not None:
    testresults.log_error(failure)
    testresults.log_trace(stacktrace)
  return testresults

def _run_test_module(name, debugOutput=True):
  if name not in _loaded_modules:
    _load_test_module(name)
  color('blue')
  print "\nLoading %s" % name

  module = _loaded_modules[name]

  setupresult = module['result']
  setupresult.printResult()

  for fun in module['Data()']:
    color('blue')
    print "\nRunning %s.%s" % (name, fun[0])
    color()
    result = _run_test_function(module, fun)
    result.printResult()
    setupresult.log_message("\n\n")
    if result.is_failure():
      setupmessage = "Test setup error in %s()" % fun[0]
      setupresult.log_error(setupmessage)
    else:
      setupmessage = "Running %s()" % fun[0]
      setupresult.log_message(setupmessage)
    setupresult.append(result)

  testresults = [ setupresult ]

  for fun in module['Test()']:
    funcname = fun[0]
    if setupresult.is_failure():
      message = "Skipping test %s.%s due to failed module initialization" % (name, funcname)
      result = Result()
      result.set_name(funcname)
      result.log_skip(message)
    else:
      color('blue')
      print "\nRunning %s.%s" % (name, funcname)
      color()
      result = _run_test_function(module, fun, xia2callRequired=True)
    result.printResult()

    color()
    testresults.append(result)

  failures = sum([1 for t in testresults if t.is_failure()])
  skips    = sum([1 for t in testresults if t.is_skipped() and not t.is_failure()])
  total    = len(testresults)
  if failures:
    color('bright', 'red')
    print "\nModule failed (%d out of %d tests failed%s)" % (failures, total, (", %d tests skipped" % skips) if skips else "")
  elif skips:
    color('bright', 'yellow')
    print "\nModule has skipped tests (%d out of %d tests skipped)" % (skips, total)
  else:
    color('bright', 'green')
    print "\nModule passed (%d tests completed successfully)" % (total)
  color()
  return testresults


if __name__ == "__main__":
  import os
  import sys
  import tests
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
    _show_all_tests()
    print "Specify tests on command line to run them"
  else:
    for t in args:
      results = _run_test_module(t)
      from junit_xml import TestSuite

      ts = TestSuite("dlstbx.qa.%s" % t, results)
      with open(os.path.join(_settings['logdir'], '%s.xml' % t), 'w') as f:
        TestSuite.to_file(f, [ts], prettyprint=False)
