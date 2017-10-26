from __future__ import absolute_import, division, print_function

# LIBTBX_SET_DISPATCHER_NAME py.test
import sys

import pytest

# modify sys.argv so the command line help shows the right executable name
sys.argv[0] = 'py.test'

exitcode = pytest.main(sys.argv[1:])
sys.exit(exitcode)
