# @Test and @Data decorator definitions
#
# These decorators are used in tests to set test parameters, such as timeouts and failure behaviour,
# but most importantly they are used for test discovery.
# @Data functions are called first. These functions should verify that the required dataset is present and valid.
# Then each @Test function is called on the dataset.

_debug = False

_discoveredTestFunctions = []
_discoveredDataFunctions = []
_disableExecution = False
_disabledCalls = []
_debug_format = "In %8s: args %s kwargs %s"

def Test(*args, **kwargs):
  """Decorator for a test function"""
  if _debug:
    print _debug_format % ("Test", args, kwargs)
  def wrap(f):
    if _debug:
      print _debug_format % (("testwrap for function %s" % f), args, kwargs)
    _discoveredTestFunctions.append((f.__name__, f, args, kwargs))

    def wrapped_f(*fargs, **fkwargs):
      if _debug:
        print _debug_format % ("wrapped_f() decorator arguments", args, kwargs)
# Function arguments will usually be empty. Theoretically the decorator could generate parameters.
#       print _debug_format % ("wrapped_f() function arguments ", fargs, fkwargs)
      if _disableExecution:
        _log_disabled_call(f.__name__)
      else:
        f(*fargs, **fkwargs)
    wrapped_f.__name__ = f.__name__ # make decorator transparent
    return wrapped_f

  # Decorator may not have parameters.
  if len(args) == 1 and callable(args[0]):
    f = args
    args = ()
    # in this case open the outer wrapping
    return wrap(*f)
  return wrap

def Data(*args, **kwargs):
  """Decorator for a data sanity checking function,
     which is run once before any tests on that dataset"""
  if _debug:
    print _debug_format % ("Data", args, kwargs)
  def wrap(f):
    if _debug:
      print _debug_format % (("datawrap for function %s" % f), args, kwargs)
    _discoveredDataFunctions.append((f.__name__, f, args, kwargs))

    def wrapped_f(*fargs, **fkwargs):
      if _debug:
        print _debug_format % ("wrapped_f() decorator arguments", args, kwargs)
# Function arguments will usually be empty. Theoretically the decorator could generate parameters.
#       print _debug_format % ("wrapped_f() function arguments ", fargs, fkwargs)
      if _disableExecution:
        _log_disabled_call(f.__name__)
      else:
        f(*fargs, **fkwargs)
    wrapped_f.__name__ = f.__name__ # make decorator transparent
    return wrapped_f

  # Decorator may not have parameters.
  if len(args) == 1 and callable(args[0]):
    f = args
    args = ()
    # in this case open the outer wrapping
    return wrap(*f)
  return wrap

def getDiscoveredTestFunctions():
  global _discoveredTestFunctions
  list = _discoveredTestFunctions
  _discoveredTestFunctions = []
  return list

def getDiscoveredDataFunctions():
  global _discoveredDataFunctions
  list = _discoveredDataFunctions
  _discoveredDataFunctions = []
  return list

def _log_disabled_call(log):
  if log not in _disabledCalls:
    _disabledCalls.append(log)

def disableDecoratorFunctions():
  global _disableExecution
  _disableExecution = True

def enableDecoratorFunctions():
  global _disableExecution
  _disableExecution = False

def disabledCalls():
  global _disabledCalls
  list = _disabledCalls
  _disabledCalls = []
  return list
