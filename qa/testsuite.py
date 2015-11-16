import comparators
from datetime import datetime
import json
import numbers
import os
import result
import shutil
import sys
import traceback
import xia2runner

# test functions for improved test readability

_debug = False


# Internal test status object, this tracks a test on the Test level
#   Module > Test (@Test function) > Subtest function (@_TestFunction call)
#             ^^ you are here ^^
_test_status = None
_test_soft_fail = 0
_test_soft_fail_tripped = False

def _reset():
  global _test_status, _test_soft_fail, _test_soft_fail_tripped
  _test_status = result.Result()
  _test_soft_fail = 0
  _test_soft_fail_tripped = False

def _output(message):
  _test_status.log_message(message)

def _fail(message):
  global _test_soft_fail_tripped
  if _test_soft_fail:
    _test_status.log_message(message)
    _test_soft_fail_tripped = True
  else:
    _test_status.log_error(message)

def _result(message, status, testOnly=False):
  if status:
    _output(" [ OK ] " + message)
  else:
    if _test_soft_fail:
      _fail(" [fail] " + message)
    else:
      _fail(" [FAIL] " + message)

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

def get_test_output():
  _resetRecursionDepth()
  r = _test_status
  _reset()
  return r

_reset()


# Embedding xia2runner: Calling, resetting, checking for test results

_xia2_was_called_in_test = False
_xia2_results = None

def _store_xia2_results(result):
  global _xia2_results
  if _debug:
    print "xia2 results stored:", result
  _xia2_results = result

def reset_xia2_results():
  global _xia2_results, _xia2_was_called_in_test
  _xia2_results = None
  _xia2_was_called_in_test = False

def get_xia2_results(required=True):
  if not _xia2_was_called_in_test:
    if required:
      fail("Test does not include xia2() call")
    return None
  if _xia2_results is None:
    if required:
      fail("xia2() results not available")
    return None
  return _xia2_results

# Useful assertions for test functions

def _assertResultsAvailable(source):
  if not _xia2_was_called_in_test:
    raise ValueError('xia2() has not been called before %s test' % source)
  if _xia2_results is None:
    raise ValueError('xia2() did not return results in %s test' % source)

def _assertParametersPresent(source, args):
  if len(args) == 0:
    raise ValueError('%s test called without parameters' % source)

def _assertNumericParameters(source, args):
  if any([not isinstance(r, numbers.Number) for r in args]):
    raise ValueError('%s test called with non-numerical parameters' % source)

def _assertNumericOrComparator(source, args):
  if any([not isinstance(r, numbers.Number) and not isinstance(r, comparators.Comparator) for r in args]):
    raise ValueError('%s test called with non-numerical parameters' % source)


# Communication with test runner qa.py: Store module reference

_module = None

def setModule(module):
 global _module
 _module = module

def getModule():
 return _module

# 'import *' should only load functions decorated with @_Export
__all__ = []

def _Export(func):
  global __all__
#  print "EXPORT ", func.__name__
  functionName = func.__name__
  __all__.append(functionName)
  def test_function_export(*args, **kwargs):
    return func(*args, **kwargs)
  test_function_export.__name__ = func.__name__ # make decorator transparent
  return test_function_export

# Another internal decorator for test functions
def _TestFunction(func):
  functionName = func.__name__
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
      e_type, e_value, e_traceback = sys.exc_info()
      stacktrace = "".join(traceback.format_tb(e_traceback)[1:])
      _test_status.log_trace("Test resulted in error: %s\n%s" % (e, stacktrace))
      result = e
      test_decorator_kwargs = getModule()['current_test'][3]
      if not ('fail_fast' in test_decorator_kwargs) or test_decorator_kwargs['fail_fast']:
        raise
    _decrementRecursionDepth()
    if softfail:
      result = not _get_soft_fail()
      if _debug:
        print "Soft fail function returns", result
    return result
  inner.__name__ = functionName # make decorator transparent
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
@_Export
def has_images(*args):
  return images(*args, override_fail=True)

@_TestFunction
@_Export
def images(*args):
  _assertParametersPresent('images', args)
  _assertNumericOrComparator('images', args)
  directory = getModule()['datadir']
  filecount = len(os.listdir(directory))
  output("Found %d files in %s" % (filecount, directory))
  for r in args:
    if isinstance(r, comparators.Comparator):
      check = r(filecount)
    else:
      check = r == filecount
    _result("Check for %s images" % r, check)

@_TestFunction
@_Export
def xia2(*args):
  global _xia2_was_called_in_test
  if _xia2_was_called_in_test:
    raise Exception('xia2 called multiple times within test')
  _xia2_was_called_in_test = True

  now = datetime.now()
  workdir = os.path.join(getModule()['workdir'], getModule()['current_test'][0])
  datadir = getModule()['datadir']
#  runid = ...

  if 'timeout' in getModule()['current_test'][3]:
    timeout = getModule()['current_test'][3]['timeout']
  else:
    timeout = 3600

  args = list(args)
  args.append(datadir)
  xia2result = xia2runner.runxia2(args, workdir, timeout)

  if _debug:
    print xia2result

  xia2result['json'], xia2result['json_raw'],  xia2result['xia2.error'] = None, None, None
  if xia2result['success'] and xia2result['jsonfile']:
    try:
      with open(xia2result['jsonfile'], 'r') as f:
        xia2result['json_raw'] = f.read()
        xia2result['json'] = json.loads(xia2result['json_raw'])
    except:
      xia2result['success'] = False
      xia2result['stderr'] = "Could not read xia2.json"

  if os.path.exists(os.path.join(workdir, 'xia2.error')):
    with open(os.path.join(workdir, 'xia2.error'), "r") as errorfile:
      xia2result['xia2.error'] = errorfile.read()

  if xia2result['success'] and xia2result['jsonfile'] and getModule()['archivedir']:
    archivejson = os.path.join(getModule()['archivedir'], getModule()['current_test'][0],
       "%s-%s-%04d%02d%02d-%02d%02d.json" % (getModule()['name'], getModule()['current_test'][0],
              now.year, now.month, now.day, now.hour, now.minute))
    if not os.path.exists(os.path.dirname(archivejson)):
      os.makedirs(os.path.dirname(archivejson))

    shutil.copyfile(xia2result['jsonfile'], archivejson)
    xz = xia2runner.compress_file(archivejson, debug=1)
    if not xz['success']:
      xia2result['success'] = False
      xia2result['stderr'] = "Could not archive xia2.json"
      xia2result['xz'] = xz

  _store_xia2_results(xia2result)

  if not xia2result['success']:
    error = "xia2() failed"
    if xia2result['stderr']:
      error += " with: " + xia2result['stderr']
    elif xia2result['xia2.error']:
      error += " with: " + " ".join(xia2result['xia2.error'].split("\n")[-2:-1])
    elif xia2result['stdout']:
      error += " with: " + xia2result['stdout'].split("\n")[-4]

    if not ('fail_fast' in getModule()['current_test'][3]) or getModule()['current_test'][3]['fail_fast']:
      raise Exception(error)
    else:
      fail(error)
  return xia2result['success']

@_TestFunction
@_Export
def spacegroup(*args):
  skip("Spacegroup not implemented yet")

@_TestFunction
@_Export
def unitcell(*args):
  skip("Unit cell not implemented yet")

@_TestFunction
@_Export
def resolution(*args):
  _assertResultsAvailable('resolution%s' % str(args))
  _assertParametersPresent('resolution', args)
  _assertNumericParameters('resolution', args)

  lowres = _xia2_results['json']['_crystals']['DEFAULT']['_scaler']['_scalr_statistics']["[\"AUTOMATIC\", \"DEFAULT\", \"NATIVE\"]"]['Low resolution limit'][0]
  highres = _xia2_results['json']['_crystals']['DEFAULT']['_scaler']['_scalr_statistics']["[\"AUTOMATIC\", \"DEFAULT\", \"NATIVE\"]"]['High resolution limit'][0]
  check = comparators.between(lowres, highres)
  _output("Resolution ranges from %.1f to %.1f" % (highres, lowres))
  for r in args:
    _result("Check for resolution %.2f" % r, check(r))

@_TestFunction
@_Export
def has_resolution(*args):
  return resolution(*args, override_fail=True)

def create_testfunctions(name, decorators=[], namespace=None):
  function_list = []

  def proto_has_f(*args):
    print "proto_has_f [%s]" % name
    skip("%s not implemented yet" % name)
  function_list.append((proto_has_f, "has_%s"))

  def proto_f(*args):
    print "proto_f [%s]" % name
    skip("%s not implemented yet" % name)
  function_list.append((proto_f, "%s"))

  if namespace is None: # default to caller's globals
    namespace = sys._getframe(1).f_globals
  for (f, fname) in function_list:
    fname = fname % name
    f.__name__ = fname
    for d in reversed(decorators):
      f = d(f)
    namespace[fname] = f

create_testfunctions('completeness', [_TestFunction, _Export])

@_TestFunction
@_Export
def completeness(*args):
  skip("Completeness not implemented yet")

@_TestFunction
@_Export
def multiplicity(*args):
  skip("Multiplicity not implemented yet")

@_TestFunction
@_Export
def uniquereflections(*args):
  skip("Unique reflections not implemented yet")

@_TestFunction
@_Export
def runtime(*args):
  _assertParametersPresent('runtime', args)
  _assertNumericOrComparator('runtime', args)
  _test_status.update_timer()
  runtime = _test_status.elapsed_sec
  output("Current runtime is %d seconds" % runtime)
  for r in args:
    if not isinstance(r, comparators.Comparator):
      r = comparators.at_most(r)
    check = r(runtime)
    _result("Check for %s seconds test runtime" % r, check)

@_TestFunction
@_Export
def output(*args):
  message = " ".join([str(x) for x in args])
  _output(message)

@_TestFunction
@_Export
def skip(*args):
  message = " ".join([str(x) for x in args])
  _test_status.log_skip(message)

@_TestFunction
@_Export
def fail(*args):
  message = " ".join([str(x) for x in args])
  _fail(message)

@_Export
class high_resolution():
  @_TestFunction
  def has_resolution(self, *args):
    return self.resolution(*args, override_fail=True)

  @_TestFunction
  def resolution(self, *args):
    _assertResultsAvailable('resolution%s' % str(args))
    _assertParametersPresent('resolution', args)
    _assertNumericParameters('resolution', args)

    lowres = _xia2_results['json']['_crystals']['DEFAULT']['_scaler']['_scalr_statistics']["[\"AUTOMATIC\", \"DEFAULT\", \"NATIVE\"]"]['Low resolution limit'][2]
    highres = _xia2_results['json']['_crystals']['DEFAULT']['_scaler']['_scalr_statistics']["[\"AUTOMATIC\", \"DEFAULT\", \"NATIVE\"]"]['High resolution limit'][2]
    check = comparators.between(lowres, highres)
    output("High resolution shell ranges from %.1f to %.1f" % (highres, lowres))
    for r in args:
      _result("Check for high resolution %.2f" % r, check(r))

_resultList = []
