from __future__ import absolute_import, division

import dlstbx.system_test
dlstbx.system_test.load_all_tests()
systests = dlstbx.system_test.get_all_tests()

print "Found %d system test classes" % len(systests)

print "Tests:", systests

import dlstbx.qa.result
r = dlstbx.qa.result.Result()
r.set_name('result')
r.set_classname('system_test')
r.log_message('asdf')

import junit_xml
ts = junit_xml.TestSuite("dlstbx.system_test", [r])
with open('output.xml', 'w') as f:
  junit_xml.TestSuite.to_file(f, [ts], prettyprint=True)
