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
_archive = os.path.join(_basedir, 'archive')
_loaded_modules = {}

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
  loader["errors"] = [ "Test runs function %s() on import. Tests should not run functions on import." % n for n in decorators.disabledCalls() ]
  if not os.path.exists(loader["datadir"]):
    loader["errors"].append( "Data directory %s is missing" % loader["datadir"] )
  loader['result'] = Result(stdout="\n".join(loader['errors']),
                            stderr="\n".join(loader['errors']))
  _loaded_modules[name] = loader
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

def _run_test_function(func, xia2callRequired=False):
  import testsuite
  failure = None
  stacktrace = None
  testsuite.resetTestResults()
  try:
    func[1]()
    def derp():
      fnail
#   derp()
  except Exception as e:
    e_type, e_value, e_traceback = sys.exc_info()
    import traceback
    stacktrace = ("".join(traceback.format_tb(e_traceback)[1:])).strip()
    failure = ("Test resulted in error: %s\n%s" % (e, e.__doc__)).strip()
  if xia2callRequired:
    testsuite.checkTestResults()
  testresults = testsuite.getTestOutput()
  if failure is not None:
    testresults.append(error=True, stdout=failure + "\n" + stacktrace, stderr=failure, stacktrace=stacktrace)
  return testresults

def _color_green(string):
  if string is None:
    color()
  else:
    color('green')

def _color_red(string):
  if string is None:
    color()
  else:
    color('red')


def _run_test_module(name, debugOutput=True):
  if name not in _loaded_modules:
    _load_test_module(name)
  module = _loaded_modules[name]

  setupresult = module['result']
  setupresult.printStdout(colorFunctionStdout=_color_green, colorFunctionStderr=_color_red)

  for fun in module['Data()']:
    color('blue')
    print "\nRunning %s.%s" % (name, fun[0])
    color()
    results = _run_test_function(fun)
    results.printStdout(colorFunctionStdout=_color_green, colorFunctionStderr=_color_red)
    setupresult.append(stdout="\n\n")
    if results.error:
      setupmessage = "Test setup error in %s()" % fun[0]
      results.prepend(stdout=setupmessage, stderr=setupmessage)
    else:
      setupmessage = "Running %s()" % fun[0]
      results.prepend(stdout=setupmessage)
    setupresult.append(results)

  testresults = { "__init__" : setupresult }
#  setupresult.printStdout(colorFunctionStdout=_color_green, colorFunctionStderr=_color_red)

  for fun in module['Test()']:
    funcname = fun[0]
    if setupresult.error:
      color('blue')
      message = "Skipping test %s.%s due to failed initialization" % (name, funcname)
      print "\n" + message
      results = Result(error=True, stdout=message, stderr=message)
    else:
      color('blue')
      print "\nRunning %s.%s" % (name, funcname)
      color()
      results = _run_test_function(fun, xia2callRequired=True)
      results.printStdout(colorFunctionStdout=_color_green, colorFunctionStderr=_color_red)

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
#  import atexit
#  def goodbye(name, adjective):
#    print 'Goodbye, %s, it was %s to meet you.' % (name, adjective)
#  atexit.register(goodbye, 'Donny', 'nice')

  import sys
  import tests
  import decorators

  if (len(sys.argv) <= 1):
    _show_all_tests()
  else:
    for t in sys.argv[1:]:
      results = _run_test_module(t)
      from junit_xml import TestSuite

      ts = TestSuite(t, [r.toJUnitTestCase(n) for (n, r) in results.iteritems()])
      with open('output.xml', 'w') as f:
        f.write(TestSuite.to_xml_string([ts]))
