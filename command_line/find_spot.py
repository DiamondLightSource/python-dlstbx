# LIBTBX_SET_DISPATCHER_NAME dials.find_spot

import random
row = random.randrange(24)
column = random.randrange(80)

for x in range(24):
  if x == row:
    print " " * column + "."
  else:
    print
