#
# model.py
#
#  Copyright (C) 2013 Diamond Light Source
#
#  Author: James Parkhurst
#
#  This code is distributed under the BSD license, a copy of which is
#  included in the root directory of this package.

from __future__ import division

from dials.algorithms.profile_model.interface import ProfileModelIface
from libtbx.phil import parse

phil_scope = parse('''
  nave {
    model {
      s = 0
        .type = float(value_min=0.0)
        .help = "The spread in mosaic block size"

      da = 0
        .type = float(value_min=0.0)
        .help = "The spread in unit cell size"

      w = 0
        .type = float(value_min=0.0)
        .help = "The angular spread of mosaic blocks."
    }
  }
''')

class ProfileModel(ProfileModelIface):
  ''' A class to encapsulate the profile model. '''

  # TODO maybe add beam divergence and dispersion

  def __init__(self, s, da, w):
    ''' Initialise with the parameters. '''
    from math import pi
    self._s = s
    self._da = da
    self._w = w
    assert(self._s > 0)
    assert(self._da >= 0)
    assert(self._w >= 0)

  def s(self):
    ''' Return the spread in mosaic block size. '''
    return self._s

  def da(self):
    ''' Return the spread of unitcell dimensions. '''
    return self._da

  def w(self):
    ''' Return the angular spread of mosaic blocks. '''
    return self._w

  def predict_reflections(self, experiment, dmin=None, dmax=None, **kwargs):
    ''' Predict the reflections. '''
    from dials.algorithms.spot_prediction import ScanVaryingReflectionPredictor
    from dials.array_family import flex

    # Create the scan varying reflection predictor
    predictor = ScanVaryingReflectionPredictor(
      experiment,
      dmin=dmin,
      margin=1)

    # Get length of scan
    scan_len = experiment.scan.get_num_images()
    crystal = experiment.crystal

    # Get the A matrices
    if crystal.num_scan_points == 0:
      A = [crystal.get_A() for i in range(scan_len+1)]
    else:
      assert(crystal.num_scan_points) == scan_len+1
      A = [crystal.get_A_at_scan_point(i)  for i in range(scan_len+1)]

    # Do the prediction
    result = predictor.for_ub(flex.mat3_double(A))
    if dmax is not None:
      assert(dmax > 0)
      result.compute_d_single(experiment)
      mask = result['d'] > dmax
      result.del_selected(mask)

    # Return the reflections
    return result

  def compute_bbox(self, experiment, reflections, **kwargs):
    ''' Compute the bounding box. '''
    raise RuntimeError("Not implemented")

  def compute_partiality(self, experiment, reflections, **kwargs):
    ''' Compute the partiality. '''
    raise RuntimeError("Not implemented")

  def compute_mask(self, experiment, reflections, **kwargs):
    ''' Compute the shoebox mask. '''
    raise RuntimeError("Not implemented")

  def dump(self):
    ''' Dump the profile model to phil parameters. '''
    from dials.algorithms.profile_model import factory
    phil_str = '''
      profile {
        nave {
          model {
            s=%g
            da=%g
            w=%g
          }
        }
      }
      ''' % (
        self.s(),
        self.da(),
        self.w())
    return factory.phil_scope.fetch(source=parse(phil_str))
