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

def _result(message, status):
  if status:
    _debug(" [ OK ] " + message)
  else:
    _fail(" [FAIL] " + message)

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
def spacegroup(*args):
  return "Not implemented yet"

@_TestFunction
def unitcell(*args):
  pass

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
