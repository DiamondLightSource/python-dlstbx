#!/usr/bin/env python
#
# image_mask.py
#
#  Copyright (C) 2013 Diamond Light Source
#
#  Author: James Parkhurst
#
#  This code is distributed under the BSD license, a copy of which is
#  included in the root directory of this package.

from __future__ import absolute_import, division, print_function

if __name__ == '__main__':

  from dials.util.options import OptionParser
  from dials.util.options import flatten_experiments
  from libtbx.phil import parse
  from dials.array_family import flex
  from dlstbx.algorithms.profile_model.nave2 import Support

  # The phil parameters
  phil_scope = parse(
  '''
    sigma_s = 0.05,0.05,0.05
      .type = floats(size=3)
    sigma_a = 0,0,0
      .type = floats(size=3)
    sigma_w = 0,0,0
      .type = floats(size=3)
    percentile = 0.99
      .type = float
    images = 0,1
      .type = ints(size=2)
  ''')

  # Parse the command line options
  parser = OptionParser(
    read_experiments=True,
    phil=phil_scope)
  params, options = parser.parse_args(show_diff_phil=True)
  experiments = flatten_experiments(params.input.experiments)
  assert(len(experiments) == 1)
  experiment = experiments[0]

  # Create the model support object
  support = Support(
    experiment.beam,
    experiment.detector,
    experiment.goniometer,
    experiment.scan,
    experiment.crystal.get_A(),
    tuple(params.sigma_s),
    tuple(params.sigma_a),
    tuple(params.sigma_w),
    params.percentile)

  # Create the image
  assert(len(experiment.detector) == 1)
  panel = experiment.detector[0]
  nimage = params.images[1] - params.images[0]
  assert(nimage > 0)
  image = [flex.int(flex.grid(panel.get_image_size()[::-1]))
           for i in range(nimage)]

  # Predict the reflections
  reflections = flex.reflection_table.from_predictions(experiment)
  for r in reflections:

    # Compute the bbox
    bbox = support.compute_bbox(
      r['panel'],
      r['s1'],
      r['xyzcal.mm'][2])

    # Create the mask
    for i in range(nimage):
      support.compute_image_mask(
        image[i],
        r['panel'],
        params.images[0] + i,
        r['s1'],
        r['xyzcal.mm'][2],
        bbox)

  # Show the image
  from matplotlib import pylab
  for i in range(nimage):
    pylab.imshow(image[i].as_numpy_array())
    pylab.show()
