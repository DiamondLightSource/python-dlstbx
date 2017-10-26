from __future__ import absolute_import, division, print_function

if __name__ == '__main__':
  from numpy.random import vonmises
  from dials.array_family import flex
  from math import cos, sin, pi
  m = 0
  k = 0.1
  k1 = 1.0 / k
  r = 1.0

  num = 1000
  z = flex.double(flex.grid(100, 100))

  for i in range(1000):
    u = vonmises(m, k1)


    for j in range(num):
      t = 2*pi*j/num
      xx = (r - r*cos(u)) + r * cos(t)
      yy = (r*sin(u)) + r * sin(t)
      ii = 40 + int(xx * 40)
      jj = 40 + int(yy * 40)
      if ii >= 0 and ii < 100 and jj >= 0 and jj < 100:
        z[jj,ii] += 1

  from matplotlib import pylab
  pylab.imshow(z.as_numpy_array())
  pylab.show()
