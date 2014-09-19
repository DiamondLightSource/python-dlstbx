from __future__ import division

try:
  from dials.framework import env
  import libtbx.load_env
  from os.path import join
  path = libtbx.env.dist_path("dlstbx")
  env.cache.add(join(path, "extensions"))
except Exception:
  pass
