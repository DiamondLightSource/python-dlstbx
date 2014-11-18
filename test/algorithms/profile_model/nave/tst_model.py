
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
    except KeyError, e:
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
    from math import cos

    s = 1000
    da = 0
    w = 0.1

    # Get stuff for each reflection
    s0 = matrix.col(self.experiment.beam.get_s0())
    s1 = self.reflections['s1']
    phi = self.reflections['xyzcal.mm'].parts()[2]
    d = self.reflections.compute_d_single(self.experiment)

    # Look through all reflections
    for i in range(len(self.reflections)):

      r = matrix.col(s1[i]) - matrix.col(s0)

      thickness = 1.0 / s
      rocking_width = d[i] / s + w
      phi0 = phi[i] - rocking_width / 2.0
      phi1 = phi[i] + rocking_width / 2.0
      z0 = r.length() * cos(w)
      z1 = r.length() + thickness / 2.0

      # Create the model
      model = Model(s0, s1[i], phi[i], d[i], s, da, w)

      # Test some simple properties
      t_s0 = model.s0()
      t_s1 = model.s1()
      t_r = model.r()
      t_phi = model.phi()
      t_d = model.d()
      t_s = model.s()
      t_da = model.da()
      t_w = model.w()
      assert(t_s == s)
      assert(t_da == da)
      assert(t_w == w)
      assert(t_d == d[i])
      assert(t_phi == phi[i])
      assert(tuple_almost_equal(t_s1, s1[i]))
      assert(tuple_almost_equal(t_s0, s0))
      assert(tuple_almost_equal(t_r, r))

      # Test some more properties
      t_thickness = model.thickness()
      t_rocking_width = model.rocking_width()
      t_phi0 = model.phi0()
      t_phi1 = model.phi1()
      t_z0 = model.z0()
      t_z1 = model.z1()
      assert(almost_equal(t_thickness, thickness))
      assert(almost_equal(t_rocking_width, rocking_width))
      assert(almost_equal(t_phi0, phi0))
      assert(almost_equal(t_phi1, phi1))
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
