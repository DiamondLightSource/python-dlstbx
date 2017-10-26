from __future__ import absolute_import, division

from math import exp, sqrt

from dials.array_family import flex
from matplotlib import pylab
from scitbx import matrix

e1 = matrix.col((1, 0)).normalize()
e2 = matrix.col((1, 2)).normalize()

E = matrix.sqr((
  e1[0], e2[0],
  e1[1], e2[1]))

E1 = E.inverse()

SIG = matrix.sqr((
  0.1**2, 0,
  0, 0.1**2))
SIG1 = SIG.inverse()

data1 = flex.double(flex.grid(100, 100))
data2 = flex.double(flex.grid(100, 100))

for j in range(100):
  for i in range(100):
    y = -1.0 + 2.0 * j / 100.0
    x = -1.0 + 2.0 * i / 100.0
    v = matrix.col((x, y))
    e1v = E1 * v
    md = (e1v.transpose() * SIG1 * e1v)[0]
    f = exp(-0.5 * md)
    data1[j,i] = f

E1S1 = E1.transpose() * SIG1 * E1

for j in range(100):
  for i in range(100):
    y = -1.0 + 2.0 * j / 100.0
    x = -1.0 + 2.0 * i / 100.0
    v = matrix.col((x, y))
    md = (v.transpose() * E1S1 * v)[0]
    f = exp(-0.5 * md)
    data2[j,i] = f

print flex.max(flex.abs(data1 - data2))

pylab.imshow(data2.as_numpy_array(), origin='bottom')
pylab.show()
pylab.imshow(data1.as_numpy_array(), origin='bottom')
pylab.show()
