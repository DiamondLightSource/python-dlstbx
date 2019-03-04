from __future__ import absolute_import, division, print_function

from dlstbx.util.version import dlstbx_version

print(dlstbx_version())
try:
    import ispyb

    print("ispyb", ispyb.__version__)
except ImportError:
    pass
try:
    import workflows

    print("workflows", workflows.__version__)
except ImportError:
    pass
try:
    import zocalo

    print("zocalo", zocalo.__version__)
except ImportError:
    pass
