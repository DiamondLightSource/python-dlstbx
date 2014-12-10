
from math import acos, atan2, sqrt, cos, sin, tan

x = []
y = []
z = 10.0

x0 = 1
x1 = 2
y0 = 1
y1 = 2

num = 1000

for i in range(num):
  x.append(x0 + i / float(num))
  y.append(y0)

for j in range(num):
  x.append(x1)
  y.append(y0 + j / float(num))

for i in range(num):
  x.append(x1 - i / float(num))
  y.append(y1)

for j in range(num):
  x.append(x0)
  y.append(y1 - j / float(num))
x.append(x0)
y.append(y0)

x1 = []
y1 = []
z1 = []

r = 1

for xx, yy in zip(x, y):
  p = (atan2(yy,xx))
  t = (acos(z / sqrt(xx*xx + yy*yy + z*z)))
  x1.append(r*sin(t)*cos(p))
  y1.append(r*sin(t)*sin(p))
  z1.append(r*cos(t))

from matplotlib import pylab
# pylab.plot(x, y)
# pylab.show()

pylab.plot(x1, y1)
pylab.show()
pylab.plot(x1, z1)
pylab.show()
pylab.plot(y1, z1)
pylab.show()
