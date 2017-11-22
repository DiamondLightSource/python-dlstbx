import pkg_resources

print "Entry points:"
for entry_point in sorted(pkg_resources.iter_entry_points('workflows.services')):
  print entry_point
#  print entry_point.load()
