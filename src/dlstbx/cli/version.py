from __future__ import annotations

import sys

from dlstbx.util.version import dlstbx_version


def run():
    print(dlstbx_version())
    print(
        f"Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
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
