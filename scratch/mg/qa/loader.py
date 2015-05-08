import decorators
from result import Result
import tests
import testsuite
import term
import traceback
import os
import sys

class Loader():
  _loaded_modules = {}
  _debug = False

  def __init__(self, database):
    self.db = database

  def load_test_module(self, name, datadir, workdir=None, archivedir=None):
    if name in self._loaded_modules:
      self._loaded_modules[name]['workdir'] = workdir
      self._loaded_modules[name]['archivedir'] = archivedir
      return self._loaded_modules[name]

    module = { 'name': name, 'datadir': datadir, 'workdir': workdir, 'archivedir': archivedir }

    module['result'] = Result()
    decorators.disableDecoratorFunctions()
    module['module'] = tests.load_module(name)
    decorators.enableDecoratorFunctions()
    module['Test()'] = decorators.getDiscoveredTestFunctions()
    module['Data()'] = decorators.getDiscoveredDataFunctions()
    module['errors'] = [ "Test runs function %s() on import. Tests should not run functions on import." % n for n in decorators.disabledCalls() ]

    if not os.path.exists(module['datadir']):
      module['errors'].append( "Data directory %s is missing" % module["datadir"] )

    for e in module['errors']:
      module['result'].log_error(e)
    module['result'].set_name('__init__')

    self._loaded_modules[name] = module
    if self._debug:
      print module
    return module

  def show_all_tests(self, datadir):
    for k, _ in tests.list_all_modules().iteritems():
      test = self.load_test_module(k, os.path.join(datadir, k))
      testlist = ", ".join([name for (name, func, args, kwargs) in test["Test()"]])
      if test["errors"]:
        term.color('bright', 'red')
      print "  %25s : [ %s ]" % (k, testlist)
      if test["errors"]:
        print "  This test contains errors:"
        term.color('', 'red')
        for n in test["errors"]:
          print "    %s" % n
        print
        term.color()

  def run_test_function(self, module, func, xia2callRequired=False):
    module['currentTest'] = func
    failure = None
    stacktrace = None

    testsuite.reset_xia2_results()
    testsuite.setModule(module.copy())

    try:
      func[1]()
    except Exception as e:
      e_type, e_value, e_traceback = sys.exc_info()
      stacktrace = ("".join(traceback.format_tb(e_traceback)[1:])).strip()
      failure = "Test resulted in error: %s" % e
    if xia2callRequired:
      testsuite.check_xia2_results()
    testresults = testsuite.getTestOutput()
    testresults.set_name(func[0])

    if failure is not None:
      testresults.log_error(failure)
      testresults.log_trace(stacktrace)
    return testresults

  def run_test_module(self, name, datadir, workdir, archivedir=None, debugOutput=True):
    if name not in self._loaded_modules:
      self.load_test_module(name, datadir, workdir, archivedir)
    term.color('blue')
    print "\nLoading %s" % name

    module = self._loaded_modules[name]

    setupresult = module['result']
    setupresult.printResult()

    for fun in module['Data()']:
      term.color('blue')
      print "\nRunning %s.%s" % (name, fun[0])
      term.color()
      result = self.run_test_function(module, fun)
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
        term.color('blue')
        print "\nRunning %s.%s" % (name, funcname)
        term.color()
        result = self.run_test_function(module, fun, xia2callRequired=True)
      result.printResult()

      term.color()
      testresults.append(result)

    failures = sum([1 for t in testresults if t.is_failure()])
    skips    = sum([1 for t in testresults if t.is_skipped() and not t.is_failure()])
    total    = len(testresults)
    if failures:
      term.color('bright', 'red')
      print "\nModule failed (%d out of %d tests failed%s)" % (failures, total, (", %d tests skipped" % skips) if skips else "")
    elif skips:
      term.color('bright', 'yellow')
      print "\nModule has skipped tests (%d out of %d tests skipped)" % (skips, total)
    else:
      term.color('bright', 'green')
      print "\nModule passed (%d tests completed successfully)" % (total)
    term.color()
    return testresults
