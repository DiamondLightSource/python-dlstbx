from __future__ import division
from libtbx import test_utils
from libtbx.test_utils.pytest import discover
import libtbx.load_env

def discover_unittests(module, pattern='tst_*.py'):
  try:
    import inspect
    import os
    import sys
    import unittest
  except:
    return tuple([])

  dist_dir = libtbx.env.dist_path(module)
  found_tests = unittest.defaultTestLoader.discover(dist_dir, pattern=pattern)

  def recursive_TestSuite_to_list(suite):
    list = []
    for t in suite:
      if isinstance(t, unittest.TestSuite):
        list.extend(recursive_TestSuite_to_list(t))
      elif isinstance(t, unittest.TestCase):
        module = t.__class__.__module__
        if module == 'unittest.loader':
          # This indicates a loading error.
          # Regenerate file name and try to run file directly.
          path = t._testMethodName.replace('.', os.path.sep)
          list.append("$D/%s.py" % path)
        else:
          module = inspect.getsourcefile(sys.modules[module])
          function = "%s.%s" % (t.__class__.__name__, t._testMethodName)
          list.append([module, function])
      else:
        raise Exception("Unknown test object (%s)" % t)
    return list
  test_list = recursive_TestSuite_to_list(found_tests)
  return tuple(test_list)

tst_list = [
  # "$D/test/algorithms/profile_model/nave/tst_model.py",
] + list(discover_unittests("dlstbx")) + discover("dlstbx")

def run():
  build_dir = libtbx.env.under_build("dlstbx")
  dist_dir = libtbx.env.dist_path("dlstbx")
  test_utils.run_tests(build_dir, dist_dir, tst_list)

if (__name__ == "__main__"):
  run()
