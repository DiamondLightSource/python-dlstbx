from __future__ import absolute_import, division, print_function

import sys
import time

try:
 for i in range(1000):
  for j in range(60):
    sys.stdout.write('\r')
    sys.stdout.write('[' + j * ' ' + '@' + (60-j) * ' ' + ']')
    sys.stdout.flush()
    time.sleep(0.03)

  for j in range(60, 0, -1):
    sys.stdout.write('\r')
    sys.stdout.write('[' + j * ' ' + '@' + (60-j) * ' ' + ']')
    sys.stdout.flush()
    time.sleep(0.03)
except KeyboardInterrupt:
  print()
