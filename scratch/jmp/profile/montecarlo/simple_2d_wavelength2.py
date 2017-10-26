from __future__ import absolute_import, division, print_function

if __name__ == '__main__':

  from dials.array_family import flex
  from math import pi, exp, sqrt, cos, atan2
  mr = 0.5
  sr = 0.05
  tc = pi
  rc = 1.0

  width, height = 1000, 1000
  f = flex.double(flex.grid(height, width))

  for j in range(height):
    for i in range(width):
      x = -1.0 + 2.0*i/width
      y = -1.0 + 2.0*j/height
      r = sqrt(x*x + y*y)
      t = atan2(y, x)
      C1 = (1.0 / (sqrt(2*pi) * sr))
      C2 = (1.0 / (2 * sr**2))
      C3 = (r - 2*mr*cos(t - tc))**2
      C4 = exp(-C2*C3)
      f[j,i] = C1*C4


  from matplotlib import pylab
  pylab.imshow(f.as_numpy_array())
  pylab.show()
