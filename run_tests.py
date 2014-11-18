from __future__ import division
from libtbx import test_utils
import libtbx.load_env

tst_list = (
  "$D/test/algorithms/profile_model/nave/tst_model.py",
)

def run () :
  build_dir = libtbx.env.under_build("dlstbx")
  dist_dir = libtbx.env.dist_path("dlstbx")
  test_utils.run_tests(build_dir, dist_dir, tst_list)

if (__name__ == "__main__"):
  run()
