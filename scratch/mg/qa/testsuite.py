from result import Result
# test functions for improved test readability

_debugOutput = False


# Internal test status object

_testStatus = None

def _reset():
  global _testStatus
  _testStatus = Result()

def _debug(message):
  _testStatus.append(stdout=message)

def _fail(message):
  _testStatus.append(error=True, stdout=message, stderr=message)

def _trace(stacktrace):
  _testStatus.append(error=True, stdout=stacktrace, stacktrace=stacktrace)

def getTestOutput():
  _resetRecursionDepth()
  r = _testStatus
  _reset()
  return r

_reset()


# Communication with xia2: Injecting, resetting, checking for test results

_testResult = None

def storeTestResults(result):
  global _testResult
  print "test result stored:", result
  _testResult = result

def resetTestResults():
  global _testResult
  _testResult = None

def checkTestResults():
  if _testResult is None:
    fail("Test does not include xia2() call")

def _assertResultsAvailable(source):
  if _testResult is None:
    raise ValueError('xia2() has not been called before %s test' % source)

def _assertParametersPresent(source, args):
  if len(args) == 0:
    raise ValueError('%s test called without parameters' % source)

def _assertNumericParameters(source, args):
  import numbers
  if any([not isinstance(r, numbers.Number) for r in args]):
    raise ValueError('%s test called with non-numerical parameters' % source)


# Internal decorator for test functions
# Only export decorated test functions for '*' imports

__all__ = []
def _TestFunction(func):
  global __all__
  functionName = func.__name__
  __all__.append(functionName)
  def inner(*args, **kwargs):
    callLevel = _incrementRecursionDepth() # ignore problems reported by inner functions
    result = ''
    if _debugOutput:
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
      _trace("Test resulted in error: %s\n%s\n%s" % (e, e.__doc__, stacktrace))
      result = e
    _decrementRecursionDepth()
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
def images(n):
  _debug("Check for %d images" % n)
#  raise('asdf')
#  return fail("not implemented yet")

@_TestFunction
def spacegroup(*args):
  return "Not implemented yet"

@_TestFunction
def unitcell(*args):
  pass

@_TestFunction
def between(boundaryA, boundaryB):
  if (boundaryA > boundaryB):
    boundaryA, boundaryB = boundaryB, boundaryA
  def comparator(x):
    return (x >= boundaryA) and (x <= boundaryB)
  return comparator 

@_TestFunction
def resolution(*args):
  _assertResultsAvailable('resolution%s' % str(args))
  _assertParametersPresent('resolution', args)
  _assertNumericParameters('resolution', args)
  for r in args:
    if callable(r):
      pass
    else:
      check = between(_testResult['resolution.low'], _testResult['resolution.high'])(r)
      if check:
        _debug("Check for resolution %.2f: OK" % r)
      else:
        _fail("Check for resolution %.2f: FAIL" % r)

@_TestFunction
def completeness(*args):
  pass

@_TestFunction
def multiplicity(*args):
  pass

@_TestFunction
def uniquereflections(*args):
  pass

@_TestFunction
def runtime(*args):
  pass

@_TestFunction
def fail(*args):
  message = " ".join([str(x) for x in args])
  _fail(message)

_resultList = []
