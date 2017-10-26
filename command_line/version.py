from __future__ import absolute_import, division

from dlstbx.util.version import dlstbx_version

print dlstbx_version()
try:
  import ispyb
  print "python-ispyb", ispyb.__version__
except ImportError:
  pass
try:
  import workflows
  print "python-workflows", workflows.__version__
except ImportError:
  pass
