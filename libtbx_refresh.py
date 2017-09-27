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
  import libtbx.pkg_utils
  libtbx.pkg_utils.require('mock', '>=2.0')
  libtbx.pkg_utils.require('ispyb', '>=0.13')
  libtbx.pkg_utils.require('workflows', '>=0.54')
  libtbx.pkg_utils.require('drmaa')
  libtbx.pkg_utils.require('junit_xml')
  libtbx.pkg_utils.require('graypy', '>=0.2.14')
except ImportError:
  print "\n" * 10 + "meh. update your cctbx maybe" + "\n" * 10
