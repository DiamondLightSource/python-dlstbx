from __future__ import division, absolute_import

import os

try:
  from dlstbx.util.version import dlstbx_version
  print dlstbx_version()
except Exception:
  pass

try:
  from dials.framework import env
  import libtbx.load_env
  path = libtbx.env.dist_path("dlstbx")
  env.cache.add(os.path.join(path, "extensions"))
except Exception:
  pass

try:
  import libtbx.pip
  libtbx.pip.require('workflows', '>=0.54')
except ImportError:
  print "\n" * 10 + "meh. update your cctbx maybe" + "\n" * 10
