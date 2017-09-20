
from math import atan2, cos, exp, pi, sin, sqrt

from dials.array_family import flex
from matplotlib import pylab
from scitbx import matrix

theta0 = pi
r0 = 1.0
xc = r0 * cos(theta0)
yc = r0 * sin(theta0)
sigma = 0.05
k = 0.5#2.5

cov = matrix.sqr((0.01, 0.005,
                  0.005, 0.01)).inverse()


pab = flex.double(flex.grid(200, 200))
for j in range(200):
  for i in range(200):
    x = -2.0 + 4.0 * i / 200.0
    y = -2.0 + 4.0 * j / 200.0
    r = sqrt(x*x + y*y)
    theta = atan2(y,x)
    xc = r0*cos(theta)
    yc = r0*sin(theta)
    v = matrix.col((x, y))
    vc = matrix.col((xc, yc))
    D2 = (v - vc).transpose() * cov * (v - vc)
    pa = exp(-0.5 * D2[0])
    # pa = exp(-(1.0 / (2*sigma**2))*((x-xc)**2 + (y-yc)**2))
    pb = exp(k*cos(theta - theta0))
    # pb = 1
    pab[j,i] = pa*pb

pylab.imshow(pab.as_numpy_array())
pylab.show()
