

def run(experiment):

  from scitbx import matrix
  from math import sqrt, exp, pi
  from dials.array_family import flex

  # Do some prediction
  refl = flex.reflection_table.from_predictions(experiment)

  # Get the geometry
  s0 = matrix.col(experiment.beam.get_s0())
  m2 = matrix.col(experiment.goniometer.get_rotation_axis())
  dx = matrix.col(experiment.detector[0].get_fast_axis())
  dy = matrix.col(experiment.detector[0].get_slow_axis())
  dz = matrix.col(experiment.detector[0].get_origin())
  ub = matrix.col(experiment.crystal.get_A())
  ra = matrix.col((ub[0], ub[3], ub[6]))
  rb = matrix.col((ub[1], ub[4], ub[7]))
  rc = matrix.col((ub[2], ub[5], ub[8]))

  print ra.dot(rb)
  print ra.dot(rc)
  print rb.dot(rc)

  # Orthogonal vectors to create the profile on
  ea = ra.normalize()
  eb = ea.cross(rc.normalize())
  ec = ea.cross(eb)

  # The sigma along each axis
  sigma_a = 0.005
  sigma_b = 0.005
  sigma_c = 0.001

  # The covariance matrix for the normal distribution
  sigma = matrix.sqr((
    sigma_a**2, 0, 0,
    0, sigma_b**2, 0,
    0, 0, sigma_c**2))
  sigmam1 = sigma.inverse()

  s1 = matrix.col(refl['s1'][0])
  rlp = s1 - s0
  xc, yc, zc = refl['xyzcal.mm'][0]
  print xc, yc, zc
  data = flex.double(flex.grid(200, 200))
  for j in range(200):
    for i in range(200):
      x = xc - 10 + 20 * i/200.0
      y = yc - 10 + 20 * j/200.0
      v = x*dx + y*dy + dz
      s = v * s0.length() / v.length()
      c = 1.0 / (sqrt((2*pi)**3 * sigma.determinant()))
      sc = s0 + rlp
      d = -0.5 * (((s - sc).transpose() * sigmam1 * (s-sc))[0])
      f = c*exp(d)
      data[j,i] = f
  print flex.max(data)


  from matplotlib import pylab, cm
  pylab.imshow(data.as_numpy_array(), cmap=cm.Greys)
  pylab.show()




if __name__ == '__main__':

  from dxtbx.model.experiment.experiment_list import ExperimentListFactory

  filename = '/home/upc86896/Projects/dials/sources/dials_regression/centroid_test_data/experiments.json'

  experiments = ExperimentListFactory.from_json_file(filename)
  experiment = experiments[0]

  run(experiment)
