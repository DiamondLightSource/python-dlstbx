# test functions for improved test readability

_debug = True


# Internal test status object, this tracks a test on the Test level
#   Module > Test (@Test function) > Subtest function (@_TestFunction call) 
#             ^^ you are here ^^
_test_status = None
_test_soft_fail = 0
_test_soft_fail_tripped = False

def _reset():
  global _test_status, _test_soft_fail, _test_soft_fail_tripped
  from result import Result
  _test_status = Result()
  _test_soft_fail = 0
  _test_soft_fail_tripped = False

def _debug(message):
  _test_status.append(stdout=message)

def _fail(message):
  global _test_soft_fail_tripped
  if _test_soft_fail:
    _test_status.append(stdout=message)
    _test_soft_fail_tripped = True
  else:
    _test_status.append(error=True, stdout=message, stderr=message)

def _skip(message):
  _test_status.append(stdout=message)

def _result(message, status, testOnly=False):
  if status:
    _debug(" [ OK ] " + message)
  else:
    if _test_soft_fail:
      _fail(" [fail] " + message)
    else:
      _fail(" [FAIL] " + message)

def _trace(stacktrace):
  _test_status.append(error=True, stdout=stacktrace, stacktrace=stacktrace)

def _set_soft_fail():
  global _test_soft_fail
  if _debug:
    print "Soft fail set"
  _test_soft_fail = _test_soft_fail + 1

def _get_soft_fail():
  global _test_soft_fail, _test_soft_fail_tripped
  _test_soft_fail = _test_soft_fail - 1
  if _debug:
    print "Soft fail unset (Trip status:", _test_soft_fail_tripped, ")"
  status = _test_soft_fail_tripped
  if (_test_soft_fail == 0):
    _test_soft_fail_tripped = False
  return status

def getTestOutput():
  _resetRecursionDepth()
  r = _test_status
  _reset()
  return r

_reset()


# Embedding xia2runner: Calling, resetting, checking for test results

_testResultXia2 = False
_testResultJSON = None

def _storeTestResults(result):
  global _testResultJSON
#  print "test result stored:", result
  _testResultJSON = result

def resetTestResults():
  global _testResultJSON, _testResultXia2
  _testResultJSON = None
  _testResultXia2 = False

def checkTestResults():
  if not _testResultXia2:
    fail("Test does not include xia2() call")
  if _testResultJSON is None:
    fail("xia2() results not available")


# Useful assertions for test functions

def _assertResultsAvailable(source):
  if not _testResultXia2:
    raise ValueError('xia2() has not been called before %s test' % source)
  if _testResultJSON is None:
    raise ValueError('xia2() did not return results in %s test' % source)

def _assertParametersPresent(source, args):
  if len(args) == 0:
    raise ValueError('%s test called without parameters' % source)

def _assertNumericParameters(source, args):
  import numbers
  if any([not isinstance(r, numbers.Number) for r in args]):
    raise ValueError('%s test called with non-numerical parameters' % source)

def _assertNumericOrComparator(source, args):
  import numbers
  if any([not isinstance(r, numbers.Number) and not isinstance(r, _Comparator) for r in args]):
    raise ValueError('%s test called with non-numerical parameters' % source)


# Communication with test runner qa.py: Store module reference

_module = None

def setModule(module):
 global _module
 _module = module

def getModule():
 return _module


# Comparator class holding a function and a plain text description of it

class _Comparator():
  def __init__(self, function, description):
    self.f = function
    self.description = description

  def eval(self,x):
    return self.f(x)

  def __str__(self):
    return self.description

  def __repr__(self):
    return self.description


# Internal decorator for test functions
# Only export a few selected and all decorated test functions for '*' imports

__all__ = ['getModule']
def _TestFunction(func):
  global __all__
  functionName = func.__name__
  __all__.append(functionName)
  def inner(*args, **kwargs):
    callLevel = _incrementRecursionDepth() # ignore problems reported by inner functions
    result = ''
    if _debug:
      print "Arguments to %s were: %s, %s" % (functionName, args, kwargs)
    softfail = 'override_fail' in kwargs
    if softfail:
      del kwargs['override_fail']
      _set_soft_fail()
    if _debug:
      print "Arguments to %s were: %s, %s" % (functionName, args, kwargs)

    try:
      result = func(*args, **kwargs)
    except Exception as e:
      if callLevel > 1:
        _decrementRecursionDepth()
        raise
      import sys
      e_type, e_value, e_traceback = sys.exc_info()
      import traceback
      stacktrace = "".join(traceback.format_tb(e_traceback)[1:])
      _trace("Test resulted in error: %s\n%s" % (e, stacktrace))
      result = e
      if not ('failFast' in getModule()['currentTest'][3]) or getModule()['currentTest'][3]['failFast']:
        raise
    _decrementRecursionDepth()
    if softfail:
      result = not _get_soft_fail()
      print "Soft fail function returns", result
    return result
  return inner

_recursionDepth = 0 # to control recursive test functions

def _incrementRecursionDepth():
  global _recursionDepth
  _recursionDepth += 1
  return _recursionDepth

def _decrementRecursionDepth():
  global _recursionDepth
  _recursionDepth -= 1
  return _recursionDepth

def _resetRecursionDepth():
  global _recursionDepth
  _recursionDepth = 0


@_TestFunction
def has_images(*args):
  return images(*args, override_fail=True)

@_TestFunction
def images(*args):
  _assertParametersPresent('images', args)
  _assertNumericOrComparator('images', args)
  directory = getModule()['datadir']
  import os
  filecount = len(os.listdir(directory))
  _debug("Found %d files in %s" % (filecount, directory))
  for r in args:
    if isinstance(r, _Comparator):
      check = r.eval(filecount)
    else:
      check = r == filecount
    _result("Check for %s images" % r, check)

@_TestFunction
def xia2(*args):
  import xia2runner
  global _testResultXia2
  _testResultXia2 = True

  from datetime import datetime
  import os
  now = datetime.now()
  workdir = os.path.join(getModule()['workdir'], getModule()['currentTest'][0])
  datadir = getModule()['datadir']
  archivejson = os.path.join(getModule()['archivedir'], getModule()['currentTest'][0],
       "%s-%s-%04d%02d%02d-%02d%02d.json" % (getModule()['name'], getModule()['currentTest'][0],
              now.year, now.month, now.day, now.hour, now.minute))
  if 'timeout' in getModule()['currentTest'][3]:
    timeout = getModule()['currentTest'][3]['timeout']
  else:
    timeout = 3600

  (success, result) = xia2runner.runxia2(args, workdir, datadir, archivejson, timeout)
  if success:
    _storeTestResults(result)
  else:
    error = "xia2() failed with: %s" % result
    if not ('failFast' in getModule()['currentTest'][3]) or getModule()['currentTest'][3]['failFast']:
      raise Exception(error)
    else:
      fail(error)
  return success

@_TestFunction
def spacegroup(*args):
  skip("Spacegroup not implemented yet")

@_TestFunction
def unitcell(*args):
  skip("Unit cell not implemented yet")

@_TestFunction
def between(boundaryA, boundaryB):
  _assertNumericParameters('between', [boundaryA, boundaryB])
  if (boundaryA > boundaryB):
    boundaryA, boundaryB = boundaryB, boundaryA
  def comparator(x):
    return (x >= boundaryA) and (x <= boundaryB)
  return _Comparator(comparator, "between %d and %d" % (boundaryA, boundaryB))

@_TestFunction
def moreThan(boundary):
  _assertNumericParameters('moreThan', [boundary])
  def comparator(x):
    return (x > boundary)
  return _Comparator(comparator, "more than %d" % (boundary))

@_TestFunction
def lessThan(boundary):
  _assertNumericParameters('lessThan', [boundary])
  def comparator(x):
    return (x < boundary)
  return _Comparator(comparator, "less than %d" % (boundary))

@_TestFunction
def atLeast(boundary):
  _assertNumericParameters('atLeast', [boundary])
  def comparator(x):
    return (x >= boundary)
  return _Comparator(comparator, "at least %d" % (boundary))

@_TestFunction
def atMost(boundary):
  _assertNumericParameters('atMost', [boundary])
  def comparator(x):
    return (x <= boundary)
  return _Comparator(comparator, "at most %d" % (boundary))

@_TestFunction
def resolution(*args):
  _assertResultsAvailable('resolution%s' % str(args))
  _assertParametersPresent('resolution', args)
  _assertNumericParameters('resolution', args)
  for r in args:
    check = between(_testResult['resolution.low'], _testResult['resolution.high']).eval(r)
    _result("Check for resolution %.2f" % r, check)

@_TestFunction
def completeness(*args):
  skip("Completeness not implemented yet")

@_TestFunction
def multiplicity(*args):
  skip("Multiplicity not implemented yet")

@_TestFunction
def uniquereflections(*args):
  skip("Unique reflections not implemented yet")

@_TestFunction
def runtime(*args):
  skip("Runtime not implemented yet")

@_TestFunction
def output(*args):
  message = " ".join([str(x) for x in args])
  _debug(message)

@_TestFunction
def skip(*args):
  message = " ".join([str(x) for x in args])
  _skip(message)

@_TestFunction
def fail(*args):
  message = " ".join([str(x) for x in args])
  _fail(message)

_resultList = []
