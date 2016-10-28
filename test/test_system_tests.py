from __future__ import absolute_import, division

import dlstbx.system_test
from dlstbx.system_test.common import CommonSystemTest
import pytest

def test_validation_should_fail_on_syntax_error_in_function():
  '''Here we test that the validation of system tests actually works.
     This is as meta as it gets.'''
  # Instantiate test class. This should be valid
  t = CommonSystemTest()
  t.validate()

  def broken_function():
    undefined_name
  setattr(t, 'test_function', broken_function)

  # Now validation should fail
  with pytest.raises(NameError):
    t.validate()


def test_validation_should_fail_on_broken_function_signatures():
  '''Here we test that the validation of system tests actually works.
     This is as meta as it gets.'''
  # Instantiate test class. This should be valid
  t = CommonSystemTest()
  t.validate()

  def valid_function():
    t.send_message(
      queue='somewhere',
      header={'something': 'something'},
      message='more something'
    )
  setattr(t, 'test_function', valid_function)
  t.validate()

  # Now validation should fail
  def broken_function():
    t.send_message(
      header={'something': 'something'},
      message='more something',
      undefined_parameter=True
    )
  setattr(t, 'test_function', broken_function)
  with pytest.raises(TypeError):
    t.validate()


def test_validate_all_system_tests():
  '''Now check that all defined system tests pass validation, ie. do not
     have syntax errors and properly call messaging functions.'''
  dlstbx.system_test.load_all_tests()

  for name, cls in dlstbx.system_test.get_all_tests().iteritems():
    print "Validating", name
    cls().validate()
