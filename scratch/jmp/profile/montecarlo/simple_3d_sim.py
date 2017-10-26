from __future__ import absolute_import, division, print_function

if __name__ == '__main__':

  from numpy.random import multivariate_normal
  from random import normalvariate, uniform
  from scipy.stats import vonmises
  from scitbx import matrix
  from math import acos, asin, cos, sin, atan2, sqrt, pi

  # The distribution parameters
  wavelength_mean = 1.0123
  wavelength_sigma = 0.01
  divangle_kappa = 1.0 / 0.05
  mosangle_kappa = 1.0 / 0.005
  mossize_sigma = 0.0001
  mosuc_sigma = 0.00001

  # The mean indicent beam direction
  s0 = -matrix.col((0, 0, 1)).normalize()

  # The rotation axis
  m2 = matrix.col((1, 0, 0))

  # The detector normal
  dn = matrix.col((0, 0, 1)).normalize()
  dnp = matrix.col((0, 0, 100))

  # The lattice vector
  r0 = matrix.col((0.5, 0.5, 0.5)).normalize() * 0.5

  # The number of counts
  ncounts = 1000

  xx = []
  yy = []
  zz = []

  # Loop through all photons
  for i in range(ncounts):

    # Select a wavelength
    wavelength = normalvariate(wavelength_mean, wavelength_sigma)
    if wavelength <= 0:
      continue

    # Select a divergence
    divangle = vonmises.rvs(divangle_kappa), uniform(0, 2*pi)

    # Select a mosaic angular spread angle
    mosangle = vonmises.rvs(mosangle_kappa), uniform(0, 2*pi)

    # Select a mosaic size point
    mossize_xyz = multivariate_normal(
      [0.0, 0.0, 0.0],
      [
        [mossize_sigma, 0.0, 0.0],
        [0.0, mossize_sigma, 0.0],
        [0.0, 0.0, mossize_sigma]
      ]
    )

    # Select a mosaic spread in unit cell sizes points
    mosuc_xyz = multivariate_normal(
      [0.0, 0.0, 0.0],
      [
        [mosuc_sigma, 0.0, 0.0],
        [0.0, mosuc_sigma, 0.0],
        [0.0, 0.0, mosuc_sigma]
      ]
    )


    # The mosaic vector relative to the reciprocal lattice origin
    r = r0 + matrix.col(mossize_xyz) + matrix.col(mosuc_xyz)
    b = r.length()
    theta = acos(r[2] / b)
    phi = atan2(r[1], r[0])
    theta += mosangle[0]
    # phi += mosangle[1]
    r = matrix.col((
      b*sin(theta)*cos(phi),
      b*sin(theta)*sin(phi),
      b*cos(theta)))
    r = r.rotate(r0, mosangle[1])

    # The ewald sphere centre relative to the rl origin
    a = 1.0 / wavelength
    s = a*s0
    theta = acos(s[2] / a)
    phi = atan2(s[1], s[0])
    theta += divangle[0]
    # phi += divangle[1]
    o = matrix.col((
      a*sin(theta)*cos(phi),
      a*sin(theta)*sin(phi),
      a*cos(theta)))
    o = o.rotate(s, divangle[1])

    # Compute the angle at which the point intersects the sphere
    a = 2*(m2.cross(r).dot(o))
    b = 2*(r.dot(o) - 2*m2.dot(r)*m2.dot(o))
    c = 2*(m2.dot(r)*m2.dot(o))
    d = a*a
    e = r.length_sq() - c + b
    f = r.length_sq() - c - b
    g = d - e*f
    if g < 0:
      continue
    h = sqrt(g)
    t1 = (a + h) / e
    t2 = (a - h) / e
    cosx1 = (1 - t1**2) / (1 + t1**2)
    cosx2 = (1 - t2**2) / (1 + t2**2)
    sinx1 = (2*t1) / (1 + t1**2)
    sinx2 = (2*t2) / (1 + t2**2)
    angle1 = atan2(sinx1, cosx1)
    angle2 = atan2(sinx2, cosx2)

    # Rotate the vector
    rp1 = r.rotate(m2, angle1)
    rp2 = r.rotate(m2, angle2)


    # Get the diffracted beam vector
    l0 = o
    l = rp1 - o
    d = (dnp - l0).dot(dn) / (l.dot(dn))
    v = d*l + l0
    x1 = v.dot(matrix.col((1, 0, 0)))
    y1 = v.dot(matrix.col((0, 1, 0)))
    l0 = o
    l = rp2 - o
    d = (dnp - l0).dot(dn) / (l.dot(dn))
    v = d*l + l0
    x2 = v.dot(matrix.col((1, 0, 0)))
    y2 = v.dot(matrix.col((0, 1, 0)))
    xx.append(x1)
    xx.append(x2)
    yy.append(y1)
    zz.append(angle1)
    yy.append(y2)
    zz.append(angle2)

  from matplotlib import pylab
  from mpl_toolkits.mplot3d import Axes3D
  fig = pylab.figure()
  ax = fig.add_subplot(111, projection='3d')
  ax.scatter(xx, yy, zz)
  ax.set_aspect("equal")
  ax.set_xlabel("X")
  ax.set_ylabel("Y")
  ax.set_zlabel("Z")
  pylab.show()
