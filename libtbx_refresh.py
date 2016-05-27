from __future__ import division

try:
  from dlstbx.util.version import dlstbx_version
  print dlstbx_version()
except Exception:
  pass

try:
  from dials.framework import env
  import libtbx.load_env
  from os.path import join
  path = libtbx.env.dist_path("dlstbx")
  env.cache.add(join(path, "extensions"))
except Exception:
  pass
