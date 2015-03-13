#!/usr/bin/env python
#
# nave_profile_model_ext.py
#
#  Copyright (C) 2013 Diamond Light Source
#
#  Author: James Parkhurst
#
#  This code is distributed under the BSD license, a copy of which is
#  included in the root directory of this package.
from __future__ import division

from dials.interfaces import ProfileModelCreatorIface


#class NaveProfileModelExt(ProfileModelCreatorIface):
#  ''' An extension class implementing the nave profile model. '''
#
#  name = 'nave'
#
#  @classmethod
#  def phil(cls):
#    from dlstbx.algorithms.profile_model.nave import phil_scope
#    return phil_scope
#
#  def __init__(self):
#    from dlstbx.algorithms.profile_model.nave import ProfileModelList
#    self._model = ProfileModelList()
#
#  @classmethod
#  def create(cls, params, experiments, reflections):
#    from dlstbx.algorithms.profile_model.nave import Factory
#    return Factory.create(params, experiments, reflections)
