from __future__ import division
from libtbx import test_utils
import libtbx.load_env

def discover_unittests():
  run_list = []
  dist_dir = libtbx.env.dist_path("dlstbx")

  try:
    import inspect
    import unittest
    import sys

    found_tests = unittest.TestLoader().discover(dist_dir, pattern='tst_*.py')

    def recursive_TestSuite_to_list(suite):
      list = []
      for t in suite:
        if isinstance(t, unittest.TestSuite):
          list.extend(recursive_TestSuite_to_list(t))
        elif isinstance(t, unittest.TestCase):
          list.append(t.id())
        else:
          raise Exception("Unknown test object (%s)" % t)
      return list
    test_list = recursive_TestSuite_to_list(found_tests)

    for t in test_list:
      test = t.split('.')
      (module, testclass, testname) = ('.'.join(test[:-2]), test[-2], test[-1])
      file = inspect.getsourcefile(sys.modules[module])
      run_list.append(['libtbx.python', file, '%s.%s' % (testclass, testname)])

  except:
    pass
  return run_list

tst_list = (
  "$D/test/algorithms/profile_model/nave/tst_model.py",
) + tuple(discover_unittests())

def run () :
  build_dir = libtbx.env.under_build("dlstbx")
  dist_dir = libtbx.env.dist_path("dlstbx")
  test_utils.run_tests(build_dir, dist_dir, tst_list)

if (__name__ == "__main__"):
  run()
