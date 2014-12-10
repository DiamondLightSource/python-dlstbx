
def quantile(m, s, x):
  from scipy.special import erfinv
  from math import sqrt
  return m + sqrt(2)*s * erfinv(2*x - 1)

if __name__ == '__main__':

  from random import uniform
  from dials.array_family import flex
  from math import pi, cos, sin
  m = 0.5
  s = 0.1

  z = flex.double(flex.grid(100, 100))

  num = 1000
  for i in range(1000):
    x = uniform(0.0, 1.0)
    r = quantile(m, s, x)
    if r <= 0:
      continue 
    for j in range(num):
      t = 2*pi*j/num
      xx = (m - r) + r * cos(t)
      yy = r * sin(t)
      ii = 50 + int(xx * 50)
      jj = 50 + int(yy * 50)
      if ii >= 0 and ii < 100 and jj >= 0 and jj < 100:
        z[jj,ii] += 1

  from matplotlib import pylab
  pylab.imshow(z.as_numpy_array())
  pylab.show()
