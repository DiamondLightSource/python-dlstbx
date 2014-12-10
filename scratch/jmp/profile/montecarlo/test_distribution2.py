

def run(experiment):

  from scitbx import matrix
  from math import sqrt, exp, pi
  from dials.array_family import flex
  from dlstbx.algorithms.profile_model.nave2 import Model
  from scipy.stats import chi2

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

  xnc, ync = experiment.detector[0].get_normal_origin()
  dist = experiment.detector[0].get_distance()

  # The sigma along each axis
  sigma_a = 0.01
  sigma_b = 0.01
  sigma_c = 0.01

  # The covariance matrix for the normal distribution
  sigma = matrix.sqr((
    sigma_a**2, 0, 0,
    0, sigma_b**2, 0,
    0, 0, sigma_c**2))
  sigmam1 = sigma.inverse()

  sigma_w_a = 0.01
  sigma_w_b = 0.01
  sigma_w_c = 0.01

  d = matrix.sqr(experiment.detector[0].get_d_matrix())
  A = ub


  h = matrix.col(refl['miller_index'][0])
  s1= refl['s1'][0]
  phi = refl['xyzcal.mm'].parts()[2][0]
  model = Model(d, A, s0, m2, s1, phi, (sigma_a, sigma_b, sigma_c), (0, 0, 0), (0.05, 0.05, 0.05))

  r = A*h

  sss = matrix.sqr((
    h[0]*h[0]*0.1*0.1, 0, 0,
    0, h[1]*h[1]*0.2*0.2,0,
    0,0,h[2]*h[2]*0.3*0.3))

  AA = A*sss*A.transpose()

  
  print sss
  h2 = A.inverse() * r
  sss =matrix.sqr((
    0.1*0.1*h2[0]*h2[0],0,0,
    0,0.2*0.2*h2[1]*h2[1],0,
    0,0,0.3*0.3*h2[2]*h2[2]))

  BB = A*sss*A.transpose()
  # BB = BB*r

  print AA
  print BB
  
  rn = r.normalize()
  if abs(rn[0]) > abs(rn[2]):
    v1 = matrix.col((-rn[1], rn[0], 0)).normalize()
  else:
    v1 = matrix.col((0, -rn[2], rn[1])).normalize()
  v2 = rn.cross(v1).normalize()
  v3 = rn.cross(v2).normalize()
  U = matrix.sqr((
    v2[0], v3[0], rn[0],
    v2[1], v3[1], rn[1],
    v2[2], v3[2], rn[2]
  ))

  ah = matrix.col((
    matrix.col((A[0], A[3], A[6])).length() * h[0],
    matrix.col((A[1], A[4], A[7])).length() * h[1],
    matrix.col((A[2], A[5], A[8])).length() * h[2]
  ))

  
  w = abs(ah.dot(matrix.col((0.05, 0.05, 0.05))))

  B = matrix.sqr((
    0.05, 0, 0,
    0, 0.05, 0,
    0, 0, 0.05))
  X = A * B
  # # print X
  # # print A * matrix.col((0.05, 0, 0))
  w = (X * r).length()
  # # Z = Y*matrix.col((0.05, 0.05, 0.05))
  # print Y
  # print w
  # exit(0)


  S = matrix.sqr((
    w*w, 0, 0,
    0, w*w, 0,
    0, 0, 0))

  sigma2 = U * S * U.transpose()
  
  sigma = A*sigma*A.transpose() + sigma2
  print "B:", tuple(U)
  sigmam1 = sigma.inverse()
  # A1 = A.inverse()
  SIG1 = sigmam1
  SIG2 = SIG1
  # SIG2 = A1.transpose() * SIG1 * A1


  # SIG1 = A1.transpose() * sigmam1 * A1

  s1 = matrix.col(refl['s1'][0])
  xc, yc, zc = refl['xyzcal.mm'][0]

  # RC = m2.axis_and_angle_as_r3_rotation_matrix(zc)
  # SIG3 = RC*SIG2*RC.transpose()
  # SIG4 = s0.length_sq() * SIG3

  ddx = dx
  ddy = dy
  ddz = dx.cross(dy)

  dd = matrix.sqr((
    ddx[0], ddy[0], ddz[0],
    ddx[1], ddy[1], ddz[1],
    ddx[2], ddy[2], ddz[2]))

  # s0 = dd*s0
  # m2 = dd*m2
  # A = dd*A
  # A1 = A.inverse()

  r0 = A*h
  kappa = 0.5
  zs = 3
  ys = 201
  xs = 201
  data1 = flex.double(flex.grid(zs, ys, xs))
  data2 = flex.double(flex.grid(zs, ys, xs))
  px_sz = experiment.detector[0].get_pixel_size()[0]
  x0 = xc - 10*px_sz
  x1 = xc + 10*px_sz
  y0 = yc - 10*px_sz
  y1 = yc + 10*px_sz
  z0 = zc - 0.2*pi/180.0
  z1 = zc + 0.2*pi/180.0
  chi2v = chi2.ppf(0.6, 3)
  for k in range(zs):
    theta = z0 + (z1 - z0) * k / (zs-1.0)
    R = m2.axis_and_angle_as_r3_rotation_matrix(theta)
    SIG2 = dd.transpose()*R*SIG1*R.transpose()*dd
    # SIG2 = dd.transpose()*R*A1.transpose()*SIG1*A1*R.transpose()*dd
    # SIG2 = SIG1
    # SIG2 = R*SIG1*R.transpose()
    for j in range(ys):
      for i in range(xs):
        x = x0 + (x1 - x0) * i / (xs-1.0)
        y = y0 + (y1 - y0) * j / (ys-1.0)
        v = matrix.col((x-xnc, y-ync, dist)).normalize()
        ds = (v*s0.length() - dd.transpose()*s0)
        ds2 = ds*(1 - r.length() / ds.length())
        ds = (v*s0.length() - dd.transpose()*(s0 + R*r0))
        # v = (d * matrix.col((x, y, 1))).normalize()
        # s = v * s0.length()
        # s = s - s0
        # ds = A1*R.transpose()*s - h
        Dm = (ds.transpose() * SIG2 * ds)[0]
        f1 = exp(-0.5*Dm)
        # sp = (dd*v).normalize() * s0.length()
        # rr = R*(sp - s0)
        # f2 = exp(kappa*(r.transpose()*(rr))[0])
        data1[k,j,i] = f1#*f2
        # dm = model.Dm(x, y, theta)
        # data2[k,j,i] = (model.Dm(x, y, theta) - chi2v)
        data2[k,j,i] = model.P(x, y, theta)
        # if k == 5 and j == 100 and i == 100:
        #   print "Centre"
        #   print x, y, theta
        #   print xc, yc, zc
        #   print tuple(s)
        #   print tuple(s1)
        #   print tuple(ds)
        #   print ds.length()
        #   print Dm
        #   print "End Centre"
        
        # data[j,i] = f
  # print flex.max(data1)
  print "Max Diff: ", flex.max(flex.abs(data1 - data2))
  # data1 = data1 - data2
  print flex.max(flex.abs(data2))

  vmax=flex.max(data1)
  from matplotlib import pylab, cm
  # pylab.plot(data2.as_numpy_array()[5,101,:])
  # pylab.show()
  for j in range(zs):
    pylab.imshow(data1.as_numpy_array()[j,:,:], cmap=cm.gist_heat,vmax=vmax)
    pylab.show()
      



if __name__ == '__main__':

  from dxtbx.model.experiment.experiment_list import ExperimentListFactory

  filename = '/home/upc86896/Projects/dials/sources/dials_regression/centroid_test_data/experiments.json'

  experiments = ExperimentListFactory.from_json_file(filename)
  experiment = experiments[0]

  run(experiment)
