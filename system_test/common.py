from __future__ import absolute_import, division
import mock
import string
import uuid

class SafeDict(dict):
  '''A dictionary that returns undefined keys as {keyname}.
     This can be used to selectively replace variables in datastructures.'''
  def __missing__(self, key):
    return '{' + key + '}'

class CommonSystemTest(object):
  '''Base class for system tests for Zocalo,
     the Diamond Light Source data analysis framework.
  '''

  guid = str(uuid.uuid4())
  '''A random unique identifier for tests. A new one will be generated for
     each invocation of a test function.'''

  parameters = SafeDict()
  '''Set of known test parameters. Generally only a unique test identifier,
     parameters['guid'], will be set.'''

  validation = False
  '''Set to true when test functions are only called for validation rather than
     testing. Think of this as 'dummy_mode'.'''

  development_mode = False
  '''A flag to distinguish between testing the live system and testing the
     development system. This should be used only sparingly, after all tests
     should be as realistic as possible, but may be required in some places,
     eg. to decide where to load external files from.'''

  def __init__(self, dev_mode=False):
    '''Constructor via which the development mode can be set.'''
    self.development_mode = dev_mode

  def enumerate_test_functions(self):
    '''Returns a list of (name, function) tuples for all declared test
       functions in the class.'''
    return [ (function, getattr(self, function))
             for function in dir(self)
             if function.startswith('test_') ]

  def validate(self):
    '''Checks that all test functions parse correctly to pick up syntax errors.
       Does run test functions with disabled messaging functions.'''
    # Replace messaging functions by mock constructs
    patch_functions = ['_add_timer', '_messaging']
    original_functions = { (x, getattr(self, x)) for x in patch_functions }
    for x in patch_functions:
      setattr(self, x, mock.create_autospec(getattr(self, x)))
    self.validation = True
    try:
      for name, function in self.enumerate_test_functions():
        print "validating", name
        function()
        self.guid = str(uuid.uuid4()) # rotate guid for next function
        print "OK"
    finally:
      # Restore messaging functions
      for name, function in original_functions:
        setattr(self, name, function)
      self.validation = False

  def collect_tests(self):
    '''Runs all test functions and collects messaging information.
       Returns a dictionary of
         { testname: { 'send': [], 'expect': [], 'timers': [], 'errors': [] } }.
    '''

    messages = {}
    for name, function in self.enumerate_test_functions():
      self.guid = str(uuid.uuid4())
      self.parameters['guid'] = self.guid
      def messaging(direction, **kwargs):
        if direction not in messages[name]:
          raise RuntimeError('Invalid messaging call (%s)' % str(direction))
        messages[name][direction].append(kwargs)
      def timer(**kwargs):
        messages[name]['timers'].append(kwargs)
      self._messaging = messaging
      self._add_timer = timer
      messages[name] = { 'send': [], 'expect': [], 'timers': [], 'errors': [] }
      try:
        function()
      except Exception, e:
        import traceback
        messages[name]['errors'].append(traceback.format_exc())
    return messages

  #
  # -- Functions for use within tests ----------------------------------------
  #

  def send_message(self, queue=None, topic=None, headers={}, message=""):
    '''Use this function within tests to send messages to queues and topics.'''
    assert queue or topic, 'Message queue or topic destination required'
    self._messaging('send', queue=queue, topic=topic, headers=headers,
                    message=message)

  def expect_message(self, queue=None, topic=None, headers=None, message=None, min_wait=0, timeout=10):
    '''Use this function within tests to wait for messages to queues and topics.'''
    assert queue or topic, 'Message queue or topic destination required'
    assert not queue or not topic, 'Can only expect message on queue or topic, not both'
    self._messaging('expect', queue=queue, topic=topic, headers=headers,
                    message=message, min_wait=min_wait, timeout=timeout)

  def timer_event(self, at_time=None, callback=None, args=None, kwargs=None):
    if args is None: args = []
    if kwargs is None: kwargs = {}
    assert at_time, 'need to specify time for event'
    assert callback, 'need to specify callback function'
    self._add_timer(at_time=at_time, callback=callback,
                    args=args, kwargs=kwargs)

  def apply_parameters(self, item):
    '''Recursively apply formatting to {item}s in a data structure, leaving
       undefined {item}s as they are.

       Examples:
         parameters = { 'x':'5' }
         recursively_replace_parameters( { '{x}': '{y}' } )
            => { '5': '{y}' }

         parameters = { 'y':'5' }
         recursively_replace_parameters( { '{x}': '{y}' } )
            => { '{x}': '5' }

         parameters = { 'x':'3', 'y':'5' }
         recursively_replace_parameters( { '{x}': '{y}' } )
            => { '3': '5' }
    '''
    if isinstance(item, basestring):
      return string.Formatter().vformat(item, (), self.parameters)
    if isinstance(item, dict):
      return { self.apply_parameters(key): self.apply_parameters(value) for
               key, value in item.iteritems() }
    if isinstance(item, tuple):
      return tuple(self.apply_parameters(list(item)))
    if isinstance(item, list):
      return [ self.apply_parameters(x) for x in item ]
    return item

  #
  # -- Internal house-keeping functions --------------------------------------
  #

  def _add_timer(self, *args, **kwargs):
    raise NotImplementedError('Test functions can not be run directly')

  def _messaging(self, *args, **kwargs):
    raise NotImplementedError('Test functions can not be run directly')

  #
  # -- Plugin-related function -----------------------------------------------
  #

  class __metaclass__(type):
    '''Define metaclass function to keep a list of all subclasses. This enables
       looking up service mechanisms by name.'''
    def __init__(cls, name, base, attrs):
      '''Add new subclass of CommonSystemTest to list of all known subclasses.'''
      if not hasattr(cls, 'register'):
        cls.register = {}
      else:
        cls.register[name] = cls
