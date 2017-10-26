from __future__ import absolute_import, division

def almost_equal(a, b, eps=1e-7):
  return abs(a - b) < eps

def tuple_almost_equal(a, b, eps=1e-7):
  res = True
  for aa, bb in zip(a, b):
    if abs(aa - bb) > eps:
      res = False
      break
  return res


class Test(object):

  def __init__(self):
    import os
    import libtbx.load_env
    from libtbx import easy_run
    from libtbx.test_utils import show_diff
    from dxtbx.model.experiment.experiment_list import ExperimentListFactory
    from dials.array_family import flex
    try:
      dials_regression = libtbx.env.dist_path('dials_regression')
    except KeyError:
      print 'FAIL: dials_regression not configured'
      exit(0)
    path = os.path.join(
      dials_regression,
      "centroid_test_data",
      "experiments.json")
    self.experiment = ExperimentListFactory.from_json_file(path)[0]
    self.reflections = flex.reflection_table.from_predictions(self.experiment)

  def run(self):
    from dlstbx.algorithms.profile_model.nave import Model
    from scitbx import matrix
    from math import cos, pi, sin, acos, asin, atan, atan2


    s = 500
    da = 0.02
    w = 0#pi/2# 0.1


    # Get stuff for each reflection
    s0 = matrix.col(self.experiment.beam.get_s0())
    m2 = matrix.col(self.experiment.goniometer.get_rotation_axis()).normalize()
    s1 = self.reflections['s1']
    phi = self.reflections['xyzcal.mm'].parts()[2]

    zz = []
    dd = []

    # Look through all reflections
    coeffs = []
    for i in range(len(self.reflections)):

      # Create the model
      model = Model(s0, m2, s1[i], phi[i], s, da, w)
      zeta = model.zeta()

      r = matrix.col(s1[i]) - matrix.col(s0)

      thickness = 1.0 / s + r.length() * da
      rocking_width = \
        w + \
        2.0 * atan2(1.0, (2.0 * s * r.length())) + \
        2.0 * atan2(0.5 * da, 1.0)
      z0 = r.length() * cos(w)
      z1 = r.length() + thickness / 2.0

      # Test some simple properties
      t_s0 = model.s0()
      t_s1 = model.s1()
      t_r = model.r()
      t_phi = model.phi()
      t_s = model.s()
      t_da = model.da()
      t_w = model.w()
      assert(t_s == s)
      assert(t_da == da)
      assert(t_w == w)
      assert(t_phi == phi[i])
      assert(tuple_almost_equal(t_s1, s1[i]))
      assert(tuple_almost_equal(t_s0, s0))
      assert(tuple_almost_equal(t_r, r))

      # Test some more properties
      t_thickness = model.thickness()
      t_rocking_width = model.rocking_width()
      t_phi0, t_phi1 = model.phi_range()
      t_z0 = model.z0()
      t_z1 = model.z1()
      assert(almost_equal(t_thickness, thickness))
      assert(almost_equal(t_rocking_width, rocking_width))
      assert(almost_equal(t_z0, t_z0))
      assert(almost_equal(t_z1, t_z1))

      # Get the angles of intersection of the ewald sphere
      t_angles = model.ewald_intersection_angles()
      angle1 = self.ewald_intersection_angle(
        s0.length(), r.length()-thickness/2.0)
      angle2 = self.ewald_intersection_angle(
        s0.length(), r.length()+thickness/2.0)
      assert(almost_equal(t_angles[0], angle1))
      assert(almost_equal(t_angles[1], angle2))

      # Test that rotation through given angles is valid and that the edge of
      # the reflection in reciprocal space touches the edge of the rotation
      if t_phi1 > t_phi0 and t_phi1 - t_phi0 < 2*pi:# and abs(zeta) < 0.3:
        r0 = r.rotate_around_origin(m2, phi[i])
        r1 = r.rotate_around_origin(m2, t_phi0)
        r2 = r.rotate_around_origin(m2, t_phi1)
        a1 = r0.angle(r1)
        a2 = r0.angle(r2)
        assert(abs(a1 - t_rocking_width * 0.5) < 1e-7)
        assert(abs(a2 - t_rocking_width * 0.5) < 1e-7)
      else:
        pass

      try:
        from dials.array_family import flex
        width, height = self.experiment.detector[0].get_image_size()
        # A,B,C,D,E,F = model.equation(self.experiment.detector[0].get_d_matrix(), t_phi)
        # image = flex.double(flex.grid(height, width))
        # for jj in range(height):
        #   for ii in range(width):
        #     x, y = self.experiment.detector[0].pixel_to_millimeter((ii,jj))
        #     V = A*x*x+B*x*y+C*y*y+D*x+E*y+F
        #     image[jj,ii] = abs(V) < 5
        # from matplotlib import pylab
        # pylab.imshow(image.as_numpy_array())
        # xp, yp, zp = self.reflections['xyzcal.mm'][i]
        # print xp, yp
        # pylab.scatter([xp],[yp], color='black')
        # pylab.show()
        # print coeff
        # width, height = self.experiment.detector[0].get_image_size_mm()
        coeff = model.parametric(self.experiment.detector[0].get_d_matrix(), t_phi)
        coeffs.append(coeff)
        # xc, yc, a, b, psi = coeff
        # print xc, yc, a, b, psi
        # from matplotlib import pylab
        # from matplotlib.patches import Ellipse
        # el = Ellipse(xy=(xc, yc), width=a, height=b, angle=psi)
        # fig = pylab.figure()
        # ax = fig.add_subplot(111, aspect='equal')
        # ax.add_artist(el)
        # ax.set_xlim(0, width)
        # ax.set_ylim(0, height)
        # pylab.show()
      except Exception:
        raise
        pass
      # xp, yp, zp = self.reflections['xyzcal.mm'][i]
      # print xp, yp
      # pylab.scatter([xp],[yp], color='black')
      # pylab.show()
      # print coeff
    width, height = self.experiment.detector[0].get_image_size_mm()
    from matplotlib import pylab
    from matplotlib.patches import Ellipse
    els = []
    for xc, yc, a, b, psi in coeffs:
      els.append(Ellipse(xy=(xc, yc), width=a, height=b, angle=psi))

    fig = pylab.figure()
    ax = fig.add_subplot(111, aspect='equal')
    for el in els:
      ax.add_artist(el)
    ax.set_xlim(0, width)
    ax.set_ylim(0, height)
    pylab.show()
      # s1s = model.minimum_box()

      # xxx = []
      # yyy = []
      # for s1ss in s1s:
      #   x, y = self.experiment.detector[0].get_ray_intersection_px(s1ss)
      #   xxx.append(x)
      #   yyy.append(y)

      # x, y = self.experiment.detector[0].get_ray_intersection_px(s1[i])
      # print x, y, xxx, yyy

      # from matplotlib import pylab
      # pylab.scatter(xxx, yyy, color='blue')
      # pylab.scatter([x], [y], color='red')
      # pylab.axis('equal')
      # pylab.show()


    # from dials.array_family import flex
    # width, height = self.experiment.detector[0].get_image_size()
    # dm = self.experiment.detector[0].get_d_matrix()
    # d0 = matrix.col((dm[0], dm[3], dm[6]))
    # d1 = matrix.col((dm[1], dm[4], dm[7]))
    # d2 = matrix.col((dm[2], dm[5], dm[8]))


    # x, y = self.reflections['xyzcal.px'].parts()[0:2]
    # d = (x - 3.0*width/4.0)**2 + (y - 3.0*height/4.0)**2
    # index = flex.min_index(d)
    # r = matrix.col(self.reflections['s1'][index]) - s0

    # K = s0.length_sq() - r.length_sq() / 2.0
    # KK = K * K
    # d0s0 = d0.dot(s0)
    # d1s0 = d1.dot(s0)
    # d2s0 = d2.dot(s0)
    # d0d2 = d0.dot(d2)
    # d1d2 = d1.dot(d2)
    # d2d2 = d2.length_sq()
    # A = d0s0*d0s0 - KK
    # B = d0s0*d1s0*2.0
    # C = d1s0*d1s0 - KK
    # D = d0s0*d2s0*2.0 - d0d2*KK*2.0
    # E = d1s0*d2s0*2.0 - d1d2*KK*2.0
    # F = d2s0*d2s0 - KK*d2d2



    # im = flex.double(flex.grid(height, width))
    # for j in range(height):
    #   for i in range(width):
    #     x, y = self.experiment.detector[0].pixel_to_millimeter((j,i))
    #     G = A*x**2 + B*x*y + C*y**2 + D*x + E*y + F
    #     # if G
    #     #   im[j,i] = True
    #     # else:
    #     #   im[j,i] = False
    #     im[j,i] = abs(G)
    #     # print G, F

    #     # if (G < -F):
    #     #   im[j,i] = True
    #     # else:
    #     #   im[j,i] = False
    # from matplotlib import pylab
    # pylab.imshow(im.as_numpy_array())
    # pylab.show()

    print 'OK'

  def ewald_intersection_angle(self, S, R):
    from math import sqrt, atan2
    h2 = R*R / (2.0 * abs(S))
    h1 = abs(S) - h2
    l = sqrt(S*S - h1*h1)
    phi3 = atan2(l, h1)
    return phi3

if __name__ == '__main__':
  test = Test()
  test.run()
