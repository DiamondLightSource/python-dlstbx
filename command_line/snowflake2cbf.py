from __future__ import division, print_function
import h5py
import shutil
from dials.array_family import flex
from dxtbx_format_nexus_ext import *

sample = None
datasets = []

def depends_on(f):

  global sample
  global datasets

  def finder(thing, path):
    global datasets
    datasets.append(path)
    if hasattr(thing, 'attrs'):
      if thing.attrs.get('NX_class', None) == 'NXsample':
        global sample
        sample = path

    if hasattr(thing, 'keys'):
      for k in thing:
        try:
          finder(thing[k], path='%s/%s' % (path, k))
        except (IOError, TypeError, ValueError, KeyError) as e:
          pass

  finder(f, path='')

def print_cbf_header(f, nn=0):

  result = []

  T = f['/entry/instrument/detector/count_time'][()]
  L = f['/entry/instrument/beam/incident_wavelength'][()]
  D = f['/entry/instrument/detector_distance'][()]
  A = f['/entry/instrument/attenuator/attenuator_transmission'][()]

  omega = f['/entry/sample/transformations/omega'][()]
  omega_increment = f['/entry/sample/transformations/omega_increment_set'][()]
  chi = f['/entry/sample/transformations/chi'][()]
  phi = f['/entry/sample/transformations/phi'][()]

  if '/entry/instrument/detector/beam_centre_x' in f:
    Bx = f['/entry/instrument/detector/beam_centre_x'][()]
    By = f['/entry/instrument/detector/beam_centre_y'][()]
  else:
    Bx = f['/entry/instrument/detector/beam_center_x'][()]
    By = f['/entry/instrument/detector/beam_center_y'][()]

  result.append('###CBF: VERSION 1.5, CBFlib v0.7.8 - Eiger detectors')
  result.append('')
  result.append('data_%06d' % (nn + 1))
  result.append('')
  result.append('''_array_data.header_convention "PILATUS_1.2"
_array_data.header_contents
;''')
  result.append('# Detector: EIGER 16M XR4i S/N 160-0001 Diamond''')
  result.append('# %s' % f['/entry/start_time'][()])
  result.append('# Pixel_size 75e-6 m x 75e-6 m')
  result.append('# Silicon sensor, thickness 0.000450 m')
  result.append('# Exposure_time %.5f s' % T)
  result.append('# Exposure_period %.5f s' % T)
  result.append('# Tau = 1e-9 s')
  result.append('# Count_cutoff 65535 counts')
  result.append('# Threshold_setting: 0 eV')
  result.append('# Gain_setting: mid gain (vrf = -0.200)')
  result.append('# N_excluded_pixels = 0')
  result.append('# Excluded_pixels: badpix_mask.tif')
  result.append('# Flat_field: (nil)')
  result.append('# Wavelength %.5f A' % L)
  result.append('# Detector_distance %.5f m' % (D / 1000.))
  result.append('# Beam_xy (%.2f, %.2f) pixels' % (Bx, By))
  result.append('# Flux 0.000000')
  result.append('# Filter_transmission %.3f' % A)
  result.append('# Start_angle %.4f deg.' % omega[nn])
  result.append('# Angle_increment %.4f deg.' % omega_increment[nn])
  result.append('# Detector_2theta 0.0000 deg.')
  result.append('# Polarization 0.990')
  result.append('# Alpha 0.0000 deg.')
  result.append('# Kappa 0.0000 deg.')
  result.append('# Phi %.4f deg.' % phi)
  result.append('# Phi_increment 0.0000 deg.')
  result.append('# Omega %.4f deg.' % omega[nn])
  result.append('# Omega_increment %.4f deg.' % omega_increment[nn])
  result.append('# Chi %.4f deg.' % chi)
  result.append('# Chi_increment 0.0000 deg.')
  result.append('# Oscillation_axis X.CW')
  result.append('# N_oscillations 1')
  result.append(';')

  return '\n'.join(result)

def pack(data):
  from cbflib_adaptbx import compress
  return compress(data)

def make_cbf(in_name, template):
  f = h5py.File(in_name, 'r')
  depends_on(f)

  global datasets

  import binascii
  start_tag = binascii.unhexlify('0c1a04d5')

  for j in range(len(f['/entry/sample/transformations/omega'][()])):
    block = 1 + (j // 1000)
    i = j % 1000
    header = print_cbf_header(f, j)
    depth, height, width = f['/entry/data/data_%06d' % block].shape
    import numpy
    data = flex.int(numpy.int32(f['/entry/data/data_%06d' % block][i]))
    good = data.as_1d() < 65536
    data.as_1d().set_selected(~good, -2)
    compressed = pack(data)

    header2 = '''

_array_data.data
;
--CIF-BINARY-FORMAT-SECTION--
Content-Type: application/octet-stream;
     conversions="x-CBF_BYTE_OFFSET"
Content-Transfer-Encoding: BINARY
X-Binary-Size: %d
X-Binary-ID: 1
X-Binary-Element-Type: "signed 32-bit integer"
X-Binary-Element-Byte-Order: LITTLE_ENDIAN
X-Binary-Number-of-Elements: %d
X-Binary-Size-Fastest-Dimension: %d
X-Binary-Size-Second-Dimension: %d
X-Binary-Size-Padding: 0

''' % (len(compressed), data.size(), data.focus()[1], data.focus()[0])


    with open(template % (i + 1), 'wb') as fout:
      print(template % (i + 1))
      fout.write(''.join(header) + header2 + start_tag + compressed)



  f.close()

if __name__ == '__main__':
  from dxtbx.format import setup_hdf5_plugin_path
  setup_hdf5_plugin_path()
  import sys
  make_cbf(sys.argv[1], sys.argv[2])
