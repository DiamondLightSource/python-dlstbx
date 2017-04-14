# LIBTBX_SET_DISPATCHER_NAME py.test
import pytest
import sys

# modify sys.argv so the command line help shows the right executable name
sys.argv[0] = 'py.test'

exitcode = pytest.main(sys.argv[1:])
sys.exit(exitcode)
