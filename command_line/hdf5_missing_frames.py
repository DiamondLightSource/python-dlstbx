#!/usr/bin/env dials.python

import h5py
import sys


def check(filename):
    f = h5py.File(filename, "r")

    n = -1
    names = ("omega", "sam_x")

    for name in names:
        try:
            n = len(f["/entry/data/%s" % name][()])
        except KeyError:
            continue

    if n == -1:
        print("Cannot find any of: %s" % (" ".join(names)))
        return 1

    for block in f["/entry/data"]:
        if not block.startswith("data_"):
            continue
        try:
            s = f["/entry/data"][block].shape
            n -= s[0]
        except KeyError as e:
            if "unable to open external file" in str(e) and "'" in str(e):
                print("Missing file: " + str(e).split("'")[1])
                continue
            raise

    print("Missing images for %s: %d" % (str(filename), n))
    return not n


if __name__ == "__main__":
    if not all(check(filename) for filename in sys.argv[1:]):
        sys.exit(1)
