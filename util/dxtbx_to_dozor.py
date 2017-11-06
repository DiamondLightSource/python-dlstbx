from __future__ import division

def dxtbx_to_dozor(hdr):

  from scitbx import matrix

  scan = hdr.get_scan()
  goniometer = hdr.get_goniometer()
  beam = hdr.get_beam()
  detector = hdr.get_detector()

  # this will end badly for I23: but then DOZOR cannot work there anyway
  assert len(detector) == 1

  detector = detector[0]

  origin = matrix.col(detector.get_origin())
  fast = matrix.col(detector.get_fast_axis())
  slow = matrix.col(detector.get_slow_axis())
  normal = fast.cross(slow)

  pixel = detector.get_pixel_size()

  dozor = { }

  # parameters which could be later overridden
  dozor['spot_size'] = 3

  # hard coded things...
  dozor['fraction_polarization'] = 0.990

  dozor['detector'] = '### FIXME'
  dozor['exposure_time'] = scan.get_exposure_times()[0]
  dozor['detector_distance'] = origin.dot(normal)
  dozor['X-ray_wavelength'] = beam.get_wavelength()

  dozor['pixel_min'] = detector.get_trusted_range()[0]
  dozor['pixel_max'] = detector.get_trusted_range()[1]

  # bad regions around the backstop? we do not have a mechanism for this at
  # this time...

  dozor['ix_min'] = 0
  dozor['ix_max'] = 0
  dozor['iy_min'] = 0
  dozor['iy_max'] = 0

  dozor['orgx'], dozor['orgy'] = detector.get_beam_centre_px(beam.get_s0())

  dozor['starting_angle'], dozor['oscillation_range'] = scan.get_oscillation()

  # FIXME override in calling code:
  dozor['first_image_number'] = 0
  dozor['image_step'] = 0
  dozor['number_images'] = 0
  dozor['name_template_image'] = ''

  return dozor

if __name__ == '__main__':
  from dxtbx import load
  import sys
  for img in sys.argv[1:]:
    print dxtbx_to_dozor(load(img))
