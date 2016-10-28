from __future__ import absolute_import, division
import mock

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

  def collect_tests(self):
    '''Runs all test functions and collects message information.'''


  def validate(self):
    '''Checks that all test functions parse correctly.
       Does run test functions with disabled messaging functions.'''
    # Replace messaging functions by mock constructs
    patch_functions = ['send_message', 'expect_message']
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

  @staticmethod
  def send_message(queue=None, topic=None, header={}, message=""):
    print "SEND!"
    pass

  @staticmethod
  def expect_message(queue=None, topic=None, header=None, content=None, timeout=None):
    print "EXPECT!"
    pass

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
