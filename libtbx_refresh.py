from __future__ import absolute_import, division, print_function

import ast
import imp
import pkgutil

import dlstbx.services
import libtbx.pkg_utils

try:
  from dlstbx.util.version import dlstbx_version
  print(dlstbx_version())
except Exception:
  pass

libtbx.pkg_utils.require('mock', '>=2.0')
libtbx.pkg_utils.require('pytest', '>=3.1')
libtbx.pkg_utils.require('ispyb', '>=4.8,<4.9')
libtbx.pkg_utils.require('workflows', '>=0.66')
libtbx.pkg_utils.require('drmaa')
libtbx.pkg_utils.require('junit_xml')
libtbx.pkg_utils.require('graypy', '>=0.2.14')
libtbx.pkg_utils.require('colorama') # is still used in one place
libtbx.pkg_utils.require('procrunner', '>=0.7.0')

# --- workflows service registration exploration ---

print("Enumerating workflow services:")
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
libtbx.pkg_utils.define_entry_points({
  'workflows.services': sorted(service_list),
  'dlstbx.wrappers': sorted([
    'big_ep = dlstbx.zocalo.wrapper.big_ep:BigEPWrapper',
    'dozor = dlstbx.zocalo.wrapper.dozor:DozorWrapper',
    'dummy = dlstbx.zocalo.wrapper:DummyWrapper',
    'edna = dlstbx.zocalo.wrapper.edna:EdnaWrapper',
    'fast_dp = dlstbx.zocalo.wrapper.fast_dp:FastDPWrapper',
    'fast_ep = dlstbx.zocalo.wrapper.fast_ep:FastEPWrapper',
    'i19screen = dlstbx.zocalo.wrapper.i19screen:I19ScreenWrapper',
    'rlv = dlstbx.zocalo.wrapper.rlv:RLVWrapper',
    'spotcounts = dlstbx.zocalo.wrapper.spot_counts_per_image:SCPIWrapper',
    'xia2 = dlstbx.zocalo.wrapper.xia2:Xia2Wrapper',
    'xia2.strategy = dlstbx.zocalo.wrapper.xia2_strategy:Xia2StrategyWrapper',
    'multi_crystal_scale = dlstbx.zocalo.wrapper.multi_crystal_scale:MultiCrystalScaleWrapper',
  ]),
})
