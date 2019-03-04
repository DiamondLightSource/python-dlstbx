# LIBTBX_SET_DISPATCHER_NAME isort

from __future__ import absolute_import, division, print_function

import re
import sys

if __name__ == "__main__":
    sys.argv[0] = re.sub(r"(-script\.pyw?|\.exe)?$", "", sys.argv[0])

    try:
        from isort.main import main
    except ImportError:
        # Install package if necessary
        import pip

        pip.main(["install", "isort"])
        from isort.main import main

    sys.exit(main())
