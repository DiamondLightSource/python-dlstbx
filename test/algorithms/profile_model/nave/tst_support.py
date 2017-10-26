
from __future__ import absolute_import, division

class Test(object):

  def __init__(self):
    from os.path import join
    import libtbx.load_env
    from dxtbx.model.experiment.experiment_list import ExperimentListFactory
    from dials.array_family import flex
    try:
      dials_regression = libtbx.env.dist_path('dials_regression')
    except KeyError:
      print 'FAIL: dials_regression not configured'
      exit(0)

    experiments = ExperimentListFactory.from_json_file(
      join(dials_regression, "centroid_test_data", "experiments.json"))

    self.experiment = experiments[0]
    self.reflections = flex.reflection_table.from_predictions(self.experiment)


  def run(self):
    from dlstbx.algorithms.profile_model.nave2 import Support, Model
    from dials.model.data import Shoebox
    from dials.algorithms.shoebox import MaskCode
    from scitbx import matrix
    from scipy.stats import chi2

    chi2p = chi2.ppf(0.99, 3)

    # Create the support class
    support = Support(
      self.experiment.beam,
      self.experiment.detector,
      self.experiment.goniometer,
      self.experiment.scan,
      self.experiment.crystal.get_A(),
      (0.02, 0.02, 0.02),
      (0, 0, 0),
      (0, 0, 0),
      0.99)

    # Process each reflections
    for i in range(len(self.reflections)):
      panel = self.reflections[i]['panel']
      s1 = self.reflections[i]['s1']
      phi = self.reflections[i]['xyzcal.mm'][2]

      p = self.experiment.detector[panel]

      D = matrix.sqr(p.get_d_matrix())
      A = matrix.sqr(self.experiment.crystal.get_A())
      s0 = matrix.col(self.experiment.beam.get_s0())
      m2 = matrix.col(self.experiment.goniometer.get_rotation_axis())
      model = Model(D, A, s0, m2, s1, phi,
                    (0.02, 0.02, 0.02),
                    (0.0, 0.0, 0.0),
                    (0.0, 0.0, 0.0))

      # Compute the bbox
      bbox = support.compute_bbox(panel, s1, phi)
      x0, x1, y0, y1, z0, z1 = bbox

      # Create the shoebox
      sbox = Shoebox(panel, bbox)
      sbox.allocate(MaskCode.Valid)

      # Compute the mask
      support.compute_mask(panel, s1, phi, sbox)

      num = sbox.mask.count(MaskCode.Valid | MaskCode.Foreground)
      assert(num > 0)

      for z in range(sbox.mask.all()[0]):
        for y in range(sbox.mask.all()[1]):
          for x in range(sbox.mask.all()[2]):
            xx0 = x + x0
            xx2 = xx0 + 1
            xx1 = (xx0 + xx2) / 2.0
            yy0 = y + y0
            yy2 = yy0 + 1
            yy1 = (yy0 + yy2) / 2.0
            zz0 = z + z0
            zz2 = zz0 + 1
            zz1 = (zz0 + zz2) / 2.0
            xy = [
              p.pixel_to_millimeter((xx0, yy0)),
              p.pixel_to_millimeter((xx0, yy1)),
              p.pixel_to_millimeter((xx0, yy2)),
              p.pixel_to_millimeter((xx1, yy0)),
              p.pixel_to_millimeter((xx1, yy1)),
              p.pixel_to_millimeter((xx1, yy2)),
              p.pixel_to_millimeter((xx2, yy0)),
              p.pixel_to_millimeter((xx2, yy1)),
              p.pixel_to_millimeter((xx2, yy2))
            ]
            zz = [
              self.experiment.scan.get_angle_from_array_index(zz0, deg=False),
              self.experiment.scan.get_angle_from_array_index(zz1, deg=False),
              self.experiment.scan.get_angle_from_array_index(zz2, deg=False)
            ]
            foreground = False
            for zzz in zz:
              for yyy in xy:
                Dm = model.Dm(yyy[0], yyy[1], zzz)
                if Dm < chi2p:
                  foreground = True
            if sbox.mask[z,y,x] == MaskCode.Valid | MaskCode.Foreground:
              assert(foreground == True)
            elif sbox.mask[z,y,x] == MaskCode.Valid | MaskCode.Background:
              assert(foreground == False)
            else:
              raise RuntimeError('Something went wrong')

    print "OK"
      #     from matplotlib import pylab
      #     for k in range(sbox.mask.all()[0]):
      #       pylab.imshow(sbox.mask.as_numpy_array()[k,:,:], interpolation='none')
      #       pylab.show()

      # # print sbox.mask.as_numpy_array()

if __name__ == '__main__':

  test = Test()
  test.run()
