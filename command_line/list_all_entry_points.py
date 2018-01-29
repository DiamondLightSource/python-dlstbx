from __future__ import absolute_import, division, print_function

import sys

import pkg_resources

print("Entry points:")
for entry_point in sorted(pkg_resources.iter_entry_points('workflows.services')):
  print(entry_point)
  if len(sys.argv)>1:
    print(entry_point.load())
    print()
