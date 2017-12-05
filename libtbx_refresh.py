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

# --- workflows service registration exploration ---

import imp, pkgutil
import dlstbx.services
import os
import sys
import workflows.services.common_service

print("Enumerating services:")
service_list = []
for _, name, _ in pkgutil.iter_modules(dlstbx.services.__path__):
  if not name.startswith('test_'):
    fid, pathname, desc = imp.find_module(name, dlstbx.services.__path__)
    module = imp.load_module(name, fid, pathname, desc)
    if fid:
      fid.close()
    for class_candidate in dir(module):
      try:
        if issubclass(getattr(module, class_candidate), workflows.services.common_service.CommonService) \
           and getattr(module, class_candidate) != workflows.services.common_service.CommonService:
          service_list.append("{classname} = dlstbx.services.{modulename}:{classname}".format(classname=class_candidate, modulename=module.__name__))
          print("  found", class_candidate)
      except TypeError:
        pass

try:
 libtbx.pkg_utils.define_entry_points({
  'workflows.services': sorted(service_list),
 })
except AttributeError:
 pass # DIALS 1.8 backwards compatibility
