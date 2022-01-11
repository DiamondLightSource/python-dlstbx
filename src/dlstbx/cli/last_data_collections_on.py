from __future__ import annotations

import warnings

from ispyb.cli import last_data_collections_on


def main(args=None):
    warnings.warn(
        "dlstbx.last_data_collections_on is deprecated, please use ispyb.last_data_collections_on instead"
    )
    last_data_collections_on.main(args=args)
