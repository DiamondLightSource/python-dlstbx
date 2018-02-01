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

import ast
import imp
import dlstbx.services
import pkgutil

print("Enumerating services:")
service_list = []
for _, name, _ in pkgutil.iter_modules(dlstbx.services.__path__):
  if not name.startswith('test_') and not name.startswith('_'):
    try:
      fid, pathname, desc = imp.find_module(name, dlstbx.services.__path__)
    except Exception:
      fid = None
    if not fid:
      print("  *** Could not read %s" % name)
      continue
    content = fid.read()
    fid.close()
    try:
      parsetree = ast.parse(content)
    except Exception:
      print("  *** Could not parse %s" % name)
      continue
    for top_level_def in parsetree.body:
      if isinstance(top_level_def, ast.ClassDef) and \
          'CommonService' in (baseclass.id for baseclass in top_level_def.bases):
        classname = top_level_def.name
        service_list.append("{classname} = dlstbx.services.{modulename}:{classname}".format(classname=classname, modulename=name))
        print("  found", classname)

try:
  libtbx.pkg_utils.define_entry_points({
    'workflows.services': sorted(service_list),
  })
except AttributeError:
  pass # DIALS 1.8 backwards compatibility
