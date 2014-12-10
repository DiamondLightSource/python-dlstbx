


if __name__ == '__main__':
  from random import normalvariate, vonmisesvariate
  from numpy.random import multivariate_normal
  import numpy
  from scitbx import matrix
  from math import cos, sin, atan2, sqrt, pi, acos, asin

  # The distribution parameters
  wavelength_mean = 1.0
  wavelength_sigma = 0.01
  divangle_mean = 0.0
  divangle_kappa = 1.0 / 0.01
  mosangle_mean = 0.0
  mosangle_kappa = 1.0 / 0.5
  mossize_mean = numpy.array([0, 0])
  mossize_sigma = numpy.array([[0.0001, 0.0], [0.0, 0.0001]])
  mosuc_mean = numpy.array([0, 0])
  mosuc_sigma = numpy.array([[0.0001, 0.0], [0.0, 0.0001]])

  # The mean indicent beam direction
  s0 = -matrix.col((1, 0)).normalize()

  # The lattice vector
  r0 = matrix.col((0.1, 0.1)).normalize() * 0.1

  # The number of counts
  ncounts = 10000

  aa = []
  xx = []
  yy = []

  # Loop through all photons
  for i in range(ncounts):
    
    # Select a wavelength
    wavelength = normalvariate(wavelength_mean, wavelength_sigma)
    if wavelength <= 0:
      continue

    # Select a divergence
    divangle = vonmisesvariate(divangle_mean, divangle_kappa)

    # Select a mosaic angular spread angle
    mosangle = vonmisesvariate(mosangle_mean, mosangle_kappa)

    # Select a mosaic size point
    mossize_xy = multivariate_normal(mossize_mean, mossize_sigma)

    # Select a mosaic spread in unit cell sizes points
    mosuc_xy = multivariate_normal(mosuc_mean, mosuc_sigma)

    # The mosaic vector relative to the reciprocal lattice origin
    r = r0 + matrix.col(mossize_xy) + matrix.col(mosuc_xy)
    r = r.rotate_2d(mosangle)

    # The ewald sphere centre relative to the rl origin
    a = 1.0 / wavelength
    o = a*s0.rotate_2d(divangle)

    # Compute the angle at which the point intersects the sphere
    b = r.length()
    if b > (2.0 * a):
      continue
    t0 = 2.0*asin(b / (2.0 * a))
    s1 = o.rotate_2d(t0)
    aa.append(t0 * 180.0 / pi)
    aa.append(-t0 * 180.0 / pi)


  from matplotlib import pylab
  # pylab.scatter(xx, yy)
  # pylab.hexbin(xx, yy, gridsize=50)
  # pylab.axis("equal")
  pylab.hist(aa, bins=100)
  pylab.show()
