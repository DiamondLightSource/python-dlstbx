#
# projection2d.py
#
#  Copyright (C) 2013 Diamond Light Source
#
#  Author: James Parkhurst
#
#  This code is distributed under the BSD license, a copy of which is
#  included in the root directory of this package.

from __future__ import division

class ProfileProjector(object):

  def __init__(self, model, experiment):
    ''' Take as input the profile model and experiment. '''

    # Check the input
    assert(len(experiment.detector) == 1)
    self.model = model
    self.experiment = experiment

    # Predict reflections
    self._reflections = self.model.predict_reflections(experiment)

  def image(self, index):
    ''' Return a projected image of the profile masks on the detector image. '''
    from dials.array_family import flex
    from dlstbx.algorithms.profile_model.nave import Projector

    # The profile model projector
    projector = Projector(
      self.experiment.beam,
      self.experiment.detector,
      self.experiment.goniometer,
      self.experiment.scan,
      self.experiment.crystal.get_A(),
      self.model.s(),
      self.model.da(),
      self.model.w())

    # Return an image with profiles projected
    return projector.image(index)
