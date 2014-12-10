

def run(experiment):

  from scitbx import matrix
  from math import sqrt, exp, pi
  from dials.array_family import flex
  from dlstbx.algorithms.profile_model.nave2 import Model

  # Do some prediction
  refl = flex.reflection_table.from_predictions(experiment)

  # Get the geometry
  s0 = matrix.col(experiment.beam.get_s0())
  m2 = matrix.col(experiment.goniometer.get_rotation_axis())
  dx = matrix.col(experiment.detector[0].get_fast_axis())
  dy = matrix.col(experiment.detector[0].get_slow_axis())
  dz = matrix.col(experiment.detector[0].get_origin())
  ub = matrix.sqr(experiment.crystal.get_A())
  ra = matrix.col((ub[0], ub[3], ub[6])).normalize()
  rb = matrix.col((ub[1], ub[4], ub[7])).normalize()
  rc = matrix.col((ub[2], ub[5], ub[8])).normalize()

  # The sigma along each axis
  sigma_a = 0.1
  sigma_b = 0.1
  sigma_c = 0.1

  # The covariance matrix for the normal distribution
  sigma = matrix.sqr((
    sigma_a**2, 0, 0,
    0, sigma_b**2, 0,
    0, 0, sigma_c**2))
  sigmam1 = sigma.inverse()

  d = matrix.sqr(experiment.detector[0].get_d_matrix())
  A = ub


  h = matrix.col(refl['miller_index'][0])

  model = Model(d, A, s0, m2, h, (sigma_a, sigma_b, sigma_c), (0, 0, 0))

  r = A*h
  
  # A = matrix.sqr((
  #   ra[0], rb[0], rc[0],
  #   ra[1], rb[1], rc[1],
  #   ra[2], rb[2], rc[2]))
  

  A1 = A.inverse()
  SIG1 = sigmam1
  # SIG1 = A1.transpose() * sigmam1 * A1
  print tuple(sigmam1)
  print tuple(A1)

  s1 = matrix.col(refl['s1'][0])
  xc, yc, zc = refl['xyzcal.mm'][0]
  zs = 11
  ys = 201
  xs = 201
  data1 = flex.double(flex.grid(zs, ys, xs))
  data2 = flex.double(flex.grid(zs, ys, xs))
  x0 = xc - 5
  x1 = xc + 5
  y0 = yc - 5
  y1 = yc + 5
  z0 = zc - 1*pi/180.0
  z1 = zc + 1*pi/180.0
  for k in range(zs):
    theta = z0 + (z1 - z0) * k / (zs-1.0)
    R = m2.axis_and_angle_as_r3_rotation_matrix(theta)
    # SIG2 = SIG1
    # SIG2 = R*SIG1*R.transpose()
    for j in range(ys):
      for i in range(xs):
        x = x0 + (x1 - x0) * i / (xs-1.0)
        y = y0 + (y1 - y0) * j / (ys-1.0)
        v = d * matrix.col((x, y, 1))
        s = v * s0.length() / v.length()
        s = s - s0
        ds = A1*R.transpose()*s - h
        Dm = (ds.transpose() * SIG1 * ds)[0]
        data1[k,j,i] = exp(-0.5*Dm)
        data2[k,j,i] = model.P(x, y, theta)
        if k == 5 and j == 100 and i == 100:
          print "Centre"
          print x, y, theta
          print xc, yc, zc
          print tuple(s)
          print tuple(s1)
          print tuple(ds)
          print ds.length()
          print Dm
          print "End Centre"
        
        # v = x*dx + y*dy + dz
        # s = v * s0.length() / v.length()
        # c = 1.0 / (sqrt((2*pi)**3 * sigma.determinant()))
        # sc = s0 + rlp
        # d = -0.5 * (((s - sc).transpose() * sigmam1 * (s-sc))[0])
        # f = c*exp(d)
        # data[j,i] = f
  print flex.max(data1)
  print "Max Diff: ", flex.max(flex.abs(data1 - data2))

  vmax=flex.max(data2)
  from matplotlib import pylab, cm
  for j in range(zs):
    pylab.imshow(data2.as_numpy_array()[j,:,:], cmap=cm.gist_heat,vmax=vmax)
    pylab.show()
      



if __name__ == '__main__':

  from dxtbx.model.experiment.experiment_list import ExperimentListFactory

  filename = '/home/upc86896/Projects/dials/sources/dials_regression/centroid_test_data/experiments.json'

  experiments = ExperimentListFactory.from_json_file(filename)
  experiment = experiments[0]

  run(experiment)
