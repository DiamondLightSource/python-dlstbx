import sys

import pkg_resources


def run():
    print("Entry points:")
    for entry_point in sorted(pkg_resources.iter_entry_points("workflows.services")):
        print(entry_point)
        if len(sys.argv) > 1:
            print(entry_point.load())
            print()
