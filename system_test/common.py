from __future__ import absolute_import, division

class CommonSystemTest(object):
  '''Framework for testing the Diamond Light Source data analysis
     'plum duff' framework.
     This is class that all system tests are derived from.
  '''

  def validate(self):
    print "OK"

  #
  # -- Plugin-related function -----------------------------------------------
  #

  class __metaclass__(type):
    '''Define metaclass function to keep a list of all subclasses. This enables
       looking up service mechanisms by name.'''
    def __init__(cls, name, base, attrs):
      '''Add new subclass of CommonSystemTest to list of all known subclasses.'''
      if not hasattr(cls, 'test_register'):
        cls.test_register = {}
      else:
        cls.test_register[name] = cls
