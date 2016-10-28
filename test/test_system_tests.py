from __future__ import absolute_import, division

def test_validate_all_system_tests():
  import dlstbx.system_test
  dlstbx.system_test.load_all_tests()

  for name, cls in dlstbx.system_test.get_all_tests().iteritems():
    print "Validating", name
    cls().validate()
