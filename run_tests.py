from __future__ import division
from libtbx import test_utils
from libtbx.test_utils.pytest import discover
import libtbx.load_env

# To write tests for dlstbx:

# 1. Test file should be named test_*.py
# 2. Test methods should be named test_*()
# 3. Nothing else needed. Rest happens by magic.

# To run dlstbx tests:

# A. libtbx.run_tests_parallel as usual
#   or, much better:
# B. run 'py.test' inside dlstbx directory

tst_list = discover("dlstbx")

def run():
  build_dir = libtbx.env.under_build("dlstbx")
  dist_dir = libtbx.env.dist_path("dlstbx")
  test_utils.run_tests(build_dir, dist_dir, tst_list)

if (__name__ == "__main__"):
  run()
