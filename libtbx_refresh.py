from __future__ import absolute_import, division, print_function

import os

try:
  from dlstbx.util.version import dlstbx_version
  print(dlstbx_version())
except Exception:
  pass

try:
  from dials.framework import env
  import libtbx.load_env
  path = libtbx.env.dist_path("dlstbx")
  env.cache.add(os.path.join(path, "extensions"))
except Exception:
  pass

import libtbx.pkg_utils
libtbx.pkg_utils.require('mock', '>=2.0')
libtbx.pkg_utils.require('pytest', '>=3.1')
libtbx.pkg_utils.require('ispyb', '>=0.13,<1.0')
libtbx.pkg_utils.require('workflows', '>=0.54')
libtbx.pkg_utils.require('drmaa')
libtbx.pkg_utils.require('junit_xml')
libtbx.pkg_utils.require('graypy', '>=0.2.14')
libtbx.pkg_utils.require('colorama') # is still used in one place
