from __future__ import absolute_import, division, print_function

import libtbx.pkg_utils

try:
  from dlstbx.util.version import dlstbx_version
  print(dlstbx_version())
except Exception:
  pass

libtbx.pkg_utils.require('mock', '>=2.0')
libtbx.pkg_utils.require('pytest', '>=3.1')
libtbx.pkg_utils.require('ispyb', '>=3.3,<3.4')
libtbx.pkg_utils.require('workflows', '>=0.54')
libtbx.pkg_utils.require('drmaa')
libtbx.pkg_utils.require('junit_xml')
libtbx.pkg_utils.require('graypy', '>=0.2.14')
libtbx.pkg_utils.require('colorama') # is still used in one place
