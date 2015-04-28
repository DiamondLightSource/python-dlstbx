from result import Result
from decorators import Data, Test
from xia2runner import xia2
from testsuite import *
from units import *
from term import *
import os

_basedir = '/dls/mx-scratch/mgerstel/qa'

_datadir = os.path.join(_basedir, 'data')
_workdir = os.path.join(_basedir, 'work')
_logdir  = os.path.join(_basedir, 'logs')
_archive = os.path.join(_basedir, 'archive')

_loaded_modules = {}
_debug = False

def _load_test_module(name):
  if name in _loaded_modules:
    return _loaded_modules[name]

  loader = { "name" : name }
  decorators.disableDecoratorFunctions()
  loader["module"] = tests.load_module(name)
  decorators.enableDecoratorFunctions()
  loader["Test()"] = decorators.getDiscoveredTestFunctions()
  loader["Data()"] = decorators.getDiscoveredDataFunctions()
  loader["runlog"] = {}
  loader["datadir"] = os.path.join(_datadir, name)
  loader["workdir"] = os.path.join(_workdir, name)
  loader["archivedir"] = os.path.join(_archive, name)
  loader["errors"] = [ "Test runs function %s() on import. Tests should not run functions on import." % n for n in decorators.disabledCalls() ]
  if not os.path.exists(loader["datadir"]):
    loader["errors"].append( "Data directory %s is missing" % loader["datadir"] )
  loader['result'] = Result(stdout="\n".join(loader['errors']),
                            stderr="\n".join(loader['errors']))
  _loaded_modules[name] = loader
  if _debug:
    print loader
  return loader

def _show_all_tests():
  print "Available tests:"
  for k, _ in tests.list_all_modules().iteritems():
    test = _load_test_module(k)
    testlist = ", ".join([name for (name, func, args, kwargs) in test["Test()"]])
    print "  %25s : [ %s ]" % (k, testlist)
    if test["errors"]:
      color('bright', 'red')
      print "  This test contains errors:"
      color('', 'red')
      for n in test["errors"]:
        print "    %s" % n
      print
      color()
  print "Specify tests on command line to run them"

def _run_test_function(module, func, xia2callRequired=False):
  import testsuite
  import timeit
  module['currentTest'] = func
  failure = None
  stacktrace = None

  testsuite.resetTestResults()
  testsuite.setModule(module.copy())

  startTime = timeit.default_timer()
  try:
    func[1]()
  except Exception as e:
    e_type, e_value, e_traceback = sys.exc_info()
    import traceback
    stacktrace = ("".join(traceback.format_tb(e_traceback)[1:])).strip()
    failure = ("Test resulted in error: %s\n%s" % (e, e.__doc__)).strip()
  if xia2callRequired:
    testsuite.checkTestResults()
  testresults = testsuite.getTestOutput()
  testresults.setTime(timeit.default_timer() - startTime)

  if failure is not None:
    testresults.append(error=True, stdout=failure + "\n" + stacktrace, stderr=failure, stacktrace=stacktrace)
  return testresults

def _run_test_module(name, debugOutput=True):
  if name not in _loaded_modules:
    _load_test_module(name)
  module = _loaded_modules[name]

  setupresult = module['result']
  setupresult.printResult()

  for fun in module['Data()']:
    color('blue')
    print "\nRunning %s.%s" % (name, fun[0])
    color()
    results = _run_test_function(module, fun)
    results.printResult()
    setupresult.append(stdout="\n\n")
    if results.error:
      setupmessage = "Test setup error in %s()" % fun[0]
      results.prepend(stdout=setupmessage, stderr=setupmessage)
    else:
      setupmessage = "Running %s()" % fun[0]
      results.prepend(stdout=setupmessage)
    setupresult.append(results)

  testresults = { "__init__" : setupresult }
#  setupresult.printResult()

  for fun in module['Test()']:
    funcname = fun[0]
    if setupresult.error:
      color('bright', 'yellow')
      message = "Skipping test %s.%s due to failed initialization" % (name, funcname)
      print "\n" + message
      results = Result(stdout=message)
      results.skip(message)
    else:
      color('blue')
      print "\nRunning %s.%s" % (name, funcname)
      color()
      results = _run_test_function(module, fun, xia2callRequired=True)
      results.printResult()

    color()
    testresults[funcname] = results

  if any([t.error for t in testresults.itervalues()]):
    color('bright', 'red')
    print "\nModule failed (%d out of %d tests failed)" % (sum([1 for t in testresults.itervalues() if t.error]), len(testresults))
  else:
    color('bright', 'green')
    print "\nModule passed (%d tests completed successfully)" % (len(testresults))
  color()
  return testresults


if __name__ == "__main__":
  import os
  import sys
  import tests
  import decorators

  print "   Base directory:", _basedir
  print "   Data directory:", _datadir
  print "   Work directory:", _workdir
  print "    Log directory:", _logdir
  print "Archive directory:", _archive
  print

  if (len(sys.argv) <= 1):
    _show_all_tests()
  else:
    for t in sys.argv[1:]:
      results = _run_test_module(t)
      from junit_xml import TestSuite

      ts = TestSuite("dlstbx.qa.%s" % t, [r.toJUnitTestCase(n) for (n, r) in results.iteritems()])
      with open(os.path.join(_logdir, '%s.xml' % t), 'w') as f:
        TestSuite.to_file(f, [ts], prettyprint=False)
