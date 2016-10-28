from __future__ import absolute_import, division
import mock
import string
import uuid

def recursive_replace(item, **kwargs):
  '''Recursively apply formatting to {item}s in a data structure, leaving
     undefined {item}s as they are.

     Example:
       recursive_replace( { '{x}': '{y}' }, x='5' )
          => { '5': '{y}' }
       recursive_replace( { '{x}': '{y}' }, y='5' )
          => { '{x}': '5' }
       recursive_replace( { '{x}': '{y}' }, x='3', y='5' )
          => { '3': '5' }
  '''
  class SafeDict(dict):
    def __missing__(self, key):
      return '{' + key + '}'
  replacement_dictionary = SafeDict(kwargs)
  def inner_replace(item):
    if isinstance(item, basestring):
      return string.Formatter().vformat(item, (), replacement_dictionary)
    if isinstance(item, dict):
      return { inner_replace(key): inner_replace(value) for
               key, value in item.iteritems() }
    if isinstance(item, tuple):
      return tuple(inner_replace(list(item)))
    if isinstance(item, list):
      return [ inner_replace(x) for x in item ]
    return item
  return inner_replace(item)

class CommonSystemTest(object):
  '''Framework for testing the Diamond Light Source data analysis
     'plum duff' framework.
     This is class that all system tests are derived from.
  '''

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
    patch_functions = ['_messaging']
    original_functions = { (x, getattr(self, x)) for x in patch_functions }
    for x in patch_functions:
      setattr(self, x, mock.create_autospec(getattr(self, x)))
    try:
      for name, function in self.enumerate_test_functions():
        print "validating", name
        function()
        print "OK"
    finally:
      # Restore messaging functions
      for name, function in original_functions:
        setattr(self, name, function)

  def collect_tests(self):
    '''Runs all test functions and collects messaging information.
       Returns a dictionary of
         { testname: { 'send': [], 'expect': [], 'errors': [] } }.

       Replaces the formatting strings in messaging calls:
         {guid} -> unique guid
    '''

    messages = {}
    for name, function in self.enumerate_test_functions():
      def messaging(direction, **kwargs):
        if direction not in messages[name]:
          raise RuntimeError('Invalid messaging call (%s)' % str(direction))
        messages[name][direction].append(kwargs)

      self._messaging = messaging
      messages[name] = { 'send': [], 'expect': [], 'errors': [] }
      print "Collecting from test function %s:" % name,
      try:
        function()
      except Exception, e:
        import traceback
        messages[name]['errors'].append(traceback.format_exc())
      if messages[name]['errors']:
        print "FAIL"
      else:
        print "OK"

      function_call_guid = uuid.uuid4()
      messages[name] = recursive_replace(messages[name], guid=function_call_guid)
    return messages

  #
  # -- Functions for use within tests ----------------------------------------
  #

  def send_message(self, queue=None, topic=None, headers={}, message=""):
    '''Use this function within tests to send messages to queues and topics.'''
    assert queue or topic, 'Message queue or topic destination required'
    self._messaging('send', queue=queue, topic=topic, headers=headers,
                    message=message)

  def expect_message(self, queue=None, topic=None, headers=None, message=None, timeout=10):
    '''Use this function within tests to wait for messages to queues and topics.'''
    assert queue or topic, 'Message queue or topic destination required'
    assert not queue or not topic, 'Can only expect message on queue or topic, not both'
    self._messaging('expect', queue=queue, topic=topic, headers=headers,
                    message=message, timeout=timeout)

  #
  # -- Internal house-keeping functions --------------------------------------
  #

  def _messaging(self, *args, **kwargs):
    assert False, 'Test functions can not be run directly'

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
