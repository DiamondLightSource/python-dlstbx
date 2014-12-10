
from __future__ import division

class Test(object):

  def __init__(self):
    from os.path import join
    import libtbx.load_env
    from dxtbx.model.experiment.experiment_list import ExperimentListFactory
    from dials.array_family import flex
    try:
      dials_regression = libtbx.env.dist_path('dials_regression')
    except KeyError, e:
      print 'FAIL: dials_regression not configured'
      exit(0)
   
    experiments = ExperimentListFactory.from_json_file(
      join(dials_regression, "centroid_test_data", "experiments.json"))

    self.experiment = experiments[0]
    self.reflections = flex.reflection_table.from_predictions(self.experiment)
    

  def run(self):
    from dlstbx.algorithms.profile_model.nave2 import Support
    from dials.model.data import Shoebox
    from dials.algorithms.shoebox import MaskCode

    # Create the support class
    support = Support(
      self.experiment.beam,
      self.experiment.detector,
      self.experiment.goniometer,
      self.experiment.scan,
      self.experiment.crystal.get_A(),
      (0.1, 0.1, 0.1),
      (0, 0, 0),
      (0, 0, 0),
      0.9999)

    # Process each reflections
    for i in range(1):#len(self.reflections)):
      panel = self.reflections[i]['panel']
      s1 = self.reflections[i]['s1']
      phi = self.reflections[i]['xyzcal.mm'][2]
      
      bbox = support.compute_bbox(panel, s1, phi)
      print bbox
      sbox = Shoebox(panel, bbox)
      sbox.allocate(MaskCode.Valid)
      
      support.compute_mask(panel, s1, phi, sbox)

      from matplotlib import pylab
      for k in range(sbox.mask.all()[0]):
        pylab.imshow(sbox.mask.as_numpy_array()[k,:,:], interpolation='none')
        pylab.show()

      # print sbox.mask.as_numpy_array()

if __name__ == '__main__':

  test = Test()
  test.run()
