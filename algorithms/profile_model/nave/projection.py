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

class Projector(object):

  def __init__(self, model, experiment):
    ''' Take as input the profile model and experiment. '''
    assert(len(experiment.detector) == 1)
    self.model = model
    self.experiment = experiment

  def image(self, index):
    ''' Return a projected image of the profile masks on the detector image. '''
    from dlstbx.algorithms.profile_model.nave.prediction import Predictor
    from dials.array_family import flex

    # Create the predictor
    predictor = Predictor(
      self.experiment.beam,
      self.experiment.detector,
      self.experiment.goniometer,
      self.experiment.scan,
      self.model.s(),
      self.model.da(),
      self.model.w())
  
    # Predict reflections on image
    reflections = predictor.on_image(index)

    # The profile model projector
    projector = ProfileProjector(
      self.experiment.beam,
      self.experiment.detector,
      self.experiment.goniometer,
      self.experiment.scan,
      self.model.s(),
      self.model.da(),
      self.model.w())

    # Return an image with profiles projected
    return projector.image(index, reflections)
