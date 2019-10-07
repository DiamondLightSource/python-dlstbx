from __future__ import absolute_import, division, print_function

import os


def get_missing_file_systems():
    return [
        fs
        for fs in (
            "/dls/i03",
            "/dls/i04",
            "/dls/i04-1",
            "/dls/i23",
            "/dls/i19-1",
            "/dls/i19-2",
            "/dls/i24",
            "/dls/mx",
        )
        if not os.path.isdir(fs)
    ]
