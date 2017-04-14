# LIBTBX_SET_DISPATCHER_NAME ptw
import sys

# modify sys.argv so the command line help shows the right executable name
sys.argv[0] = 'ptw'

from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.exit(
        load_entry_point('pytest-watch==4.1.0', 'console_scripts', 'ptw')()
    )
