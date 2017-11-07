from __future__ import division
from __future__ import print_function

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

  dozor['detector'] = 'pilatus6m'
  dozor['exposure'] = scan.get_exposure_times()[0]
  dozor['detector_distance'] = origin.dot(normal)
  dozor['X-ray_wavelength'] = beam.get_wavelength()

  dozor['pixel_min'] = int(round(detector.get_trusted_range()[0]))
  dozor['pixel_max'] = int(round(detector.get_trusted_range()[1]))

  # bad regions around the backstop? we do not have a mechanism for this at
  # this time...

  dozor['ix_min'] = 0
  dozor['ix_max'] = 1
  dozor['iy_min'] = 0
  dozor['iy_max'] = 1

  dozor['orgx'], dozor['orgy'] = detector.get_beam_centre_px(beam.get_s0())

  dozor['starting_angle'], dozor['oscillation_range'] = scan.get_oscillation()

  image_range = scan.get_image_range()

  dozor['first_image_number'] = image_range[0]
  dozor['image_step'] = 1
  dozor['number_images'] = image_range[1] - image_range[0] + 1
  dozor['name_template_image'] = ''

  return dozor

def write_dozor_input(dozor_params, fout):
  template = '''job single
detector {detector:s}
exposure {exposure:.3f}
spot_size {spot_size:d}
detector_distance {detector_distance:.3f}
X-ray_wavelength {X-ray_wavelength:.5f}
fraction_polarization {fraction_polarization:.3f}
pixel_min {pixel_min:d}
pixel_max {pixel_max:d}
ix_min {ix_min:d}
ix_max {ix_max:d}
iy_min {iy_min:d}
iy_max {iy_max:d}
orgx {orgy:.2f}
orgy {orgy:.2f}
oscillation_range {oscillation_range:.3f}
image_step {image_step:.3f}
starting_angle {starting_angle:.3f}
first_image_number {first_image_number:d}
number_images {number_images:d}
name_template_image {name_template_image:s}
end
'''

  text = template.format(**dozor_params)
  if not fout is '-':
    open(fout, 'w').write(text)
  else:
    print(text)

def parse_dozor_output(output):
  dozor_scores = {}
  for record in output.split('\n'):
    tokens = record.split()
    try:
      image = int(tokens[0])
      scores = map(float, tokens[-3:])
      dozor_scores[image] = scores
    except ValueError as e:
      continue
    except IndexError as e:
      continue
  return dozor_scores

if __name__ == '__main__':
  from dxtbx import load
  import sys
  for img in sys.argv[1:]:
    write_dozor_input(dxtbx_to_dozor(load(img)), '-')
