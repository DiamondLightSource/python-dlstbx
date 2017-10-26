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
    from dlstbx.algorithms.profile_model.nave import ProfileModelSupport
    from math import pi
    from dials.array_family import flex

    self.reflections.compute_d_single(self.experiment)

    s = 100
    da = 0
    w = 0#0.0# 0.1

    # Create the support
    support = ProfileModelSupport(
      self.experiment.beam,
      self.experiment.detector,
      self.experiment.goniometer,
      self.experiment.scan,
      s,
      da,
      w)

    # Compute and test the bounding boxes
    support.compute_bbox(self.reflections)
    bbox = self.reflections['bbox']
    xyz = self.reflections['xyzcal.px']
    for i in range(len(bbox)):
      assert(bbox[i][1] > bbox[i][0])
      assert(bbox[i][3] > bbox[i][2])
      assert(bbox[i][5] > bbox[i][4])
      assert(xyz[i][0] >= bbox[i][0])
      assert(xyz[i][1] >= bbox[i][2])
      assert(xyz[i][2] >= bbox[i][4])
      assert(xyz[i][0] <  bbox[i][1])
      assert(xyz[i][1] <  bbox[i][3])
      assert(xyz[i][2] <  bbox[i][5])

    # Create the shoeboxes
    self.reflections['shoebox'] = flex.shoebox(
      self.reflections['panel'],
      self.reflections['bbox'])
    self.reflections['shoebox'].allocate()

    # Create the mask
    support.compute_mask(self.reflections)

    width, height = self.experiment.detector[0].get_image_size()
    image = flex.int(flex.grid(height, width))
    for r in self.reflections:
      b = r['bbox']
      s = r['shoebox']
      sl = s.mask[0:1,:,:]
      sl.reshape(flex.grid(sl.all()[1], sl.all()[2]))
      if b[0] >= 0 and b[1] <= width and b[2] >= 0 and b[3] <= height:
        image[b[2]:b[3],b[0]:b[1]] = sl


    from matplotlib import pylab
    pylab.imshow(image.as_numpy_array())
    pylab.show()

    print 'OK'


if __name__ == '__main__':
  test = Test()
  test.run()
