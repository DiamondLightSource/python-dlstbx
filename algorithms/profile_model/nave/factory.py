#
# factory.py
#
#  Copyright (C) 2013 Diamond Light Source
#
#  Author: James Parkhurst
#
#  This code is distributed under the BSD license, a copy of which is
#  included in the root directory of this package.

from __future__ import absolute_import, division, print_function

class Factory(object):
  ''' A factory class to compute the profile models. '''

  @classmethod
  def compute_single(cls, experiment, reflections, min_zeta=0.05):
    ''' Compute the profile model. '''
    raise RuntimeError("Not impemented")

  @classmethod
  def compute(cls, params, experiments, reflections):
    ''' Compute the profile models. '''
    raise RuntimeError("Not implemented")

  @classmethod
  def load(cls, params):
    ''' Load from phil parameters. '''
    raise RuntimeError("Not implemented")

  @classmethod
  def create(cls, params, experiments, reflections):
    ''' Create the profile models. '''
    if len(params.profile.gaussian_rs.model) > 0:
      assert(len(params.profile.gaussian_rs.model) == len(experiments))
      model = Factory.load(params.profile)
    else:
      assert(reflections is not None)
      model = Factory.compute(params.profile, experiments, reflections)
    return model
