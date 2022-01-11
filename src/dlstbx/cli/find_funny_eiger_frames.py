from __future__ import annotations

import sys

from dxtbx import load


def get_masked_pixel_count(filename):
    images = load(filename)
    n = images.get_num_images()
    MAX = 0xFFFF
    return [(images.get_raw_data(j) == MAX).count(True) for j in range(n)]


def run():
    result = get_masked_pixel_count(sys.argv[1])
    minimum = min(result)
    for j, r in enumerate(result):
        print("%5d %8d" % (j, r - minimum))
