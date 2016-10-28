from __future__ import absolute_import, division

import dlstbx
import dlstbx.qa.result
import dlstbx.system_test
import logging

dlstbx.enable_graylog()
logger = logging.getLogger('dlstbx.system_test')
logger.setLevel(logging.DEBUG)

# Enable logging to console
console = logging.StreamHandler()
#if not debug:
#  console.setLevel(logging.INFO)
logger.addHandler(console)

dlstbx.system_test.load_all_tests()
systest_classes = dlstbx.system_test.get_all_tests()

logger.info("Found %d system test classes" % len(systest_classes))

tests = {}
for classname, cls in systest_classes.iteritems():
  logger.debug('Collecting tests from %s' % classname)
  for testname, testsetting in cls().collect_tests().iteritems():
    testresult = dlstbx.qa.result.Result()
    testresult.set_name(testname)
    testresult.set_classname(classname)
    if testsetting.get('errors'):
      testresult.log_trace("\n".join(testsetting['errors']))
    tests[(classname, testname)] = (testsetting, testresult)

print tests

import junit_xml
ts = junit_xml.TestSuite("dlstbx.system_test",
                         [r for _, r in tests.itervalues()])
with open('output.xml', 'w') as f:
  junit_xml.TestSuite.to_file(f, [ts], prettyprint=True)
