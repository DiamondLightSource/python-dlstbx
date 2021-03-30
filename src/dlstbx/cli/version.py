import sys
from dlstbx.util.version import dlstbx_version


def run():
    print(dlstbx_version())
    print("Python {0.major}.{0.minor}.{0.micro}".format(sys.version_info))
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
