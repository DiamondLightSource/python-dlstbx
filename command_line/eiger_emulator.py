# EIGER emulator

import fabio
import hashlib
import json
import lz4
from optparse import OptionParser, SUPPRESS_HELP
import os
import random
import sys
import time
import zmq

print("Current libzmq version is %s" % zmq.zmq_version())
print("Current  pyzmq version is %s" % zmq.__version__)


default_image = '/home/upc86898/Desktop/EigerStream/zmq/adsc_streamer/test/testcrystal_1_001.img'
if not os.path.exists(default_image):
  default_image = '/dls/science/users/wra62962/zmq/adsc_streamer/test/testcrystal_1_001.img'

parser = OptionParser()
parser.add_option("-?", help=SUPPRESS_HELP, action="help")
parser.add_option('-f', '--file', dest="filename",
                  help="image file to send", metavar="FILE",
                  default=default_image)
parser.add_option('-n', '--num-images', dest="numimgs", type="int",
                  help="number of images to send, default 100-130", metavar="NUM",
                  default=int(random.uniform(100, 130)))
parser.add_option('-p', '--port', dest="port", type="int",
                  help="TCP port to wait on (%default)", metavar="PORT",
                  default=5557)
parser.add_option("--basic", action="store_true", dest="header_basic", default=False,
                  help="Generate basic detail stream headers")
parser.add_option("--full", action="store_true", dest="header_full", default=False,
                  help="Generate full detail stream headers (unsupported)")

# image header parameters
parser.add_option('--exposure-time', dest='exposure_time', type="float", default=0, help=SUPPRESS_HELP)
options, args = parser.parse_args()

if options.exposure_time < 1 / 133:
  options.exposure_time = 1 / 133

# Read image data
img_file = fabio.open(options.filename)
img_header = img_file.header
img_data = lz4.dumps(img_file.data)

def message_header_none(series='08/15'):
  return [
    json.dumps({'htype':'dheader-1.0',
                'series': series,
                'header_detail': 'none'})
    ]

def message_header_basic(series='08/15'):
  return [
    json.dumps({'htype':'dheader-1.0',
                'series': series,
                'header_detail': 'basic'}),
    json.dumps({'auto_summation': True,
                'beam_center_x': random.uniform(200, 240),
                'beam_center_y': random.uniform(200, 240),
                'bit_depth_image': 16,
                'bit_depth_readout': 16,
                'chi_increment': None,
                'chi_start': None,
                'compression': 'lz4',
                'count_time': options.exposure_time,
                'countrate_correction_applied': True,
                'countrate_correction_count_cutoff': 65535,
                'data_collection_date': "Thu Sep  8 13:37:18 BST 2016",
                'description': "EIGER 16M simulator",
                'detector_distance': 160,
                'detector_number': 1,
                'detector_readout_time': None,
                'element': None,
                'flatfield': None,
                'flatfield_correction_applied': None,
                'frame_time': None,
                'kappa_increment': None,
                'kappa_start': None,
                'nimages': None,
                'ntrigger': None,
                'number_of_excluded_pixels': None,
                'omega_increment': None,
                'omega_start': None,
                'phi_increment': None,
                'phi_start': None,
                'photon_energy': None,
                'pixel_mask': None,
                'pixel_mask_applied': None,
                'roi_mode': None,
                'sensor_material': None,
                'sensor_thickness': None,
                'software_version': None,
                'threshold_energy': None,
                'trigger_mode': None,
                'two_theta_increment': None,
                'two_theta_start': None,
                'wavelength': None,
                'x_pixel_size': None,
                'x_pixels_in_detector': None,
                'y_pixel_size': None,
                'y_pixels_in_detector': None
               })
    ]

def message_image(series='08/15', frameid=0):
  start_time = time.time()
  exposure = options.exposure_time
  end_time = start_time + exposure
  in_ns = 1000*1000*1000

  image_frame = [
    None,
    json.dumps({'htype': 'dimage_d-1.0',
                'shape': [ int(img_header['SIZE1']), int(img_header['SIZE2']) ],
                'type': 'uint16',
                'encoding': 'lz4<',
                'size': len(img_data)}),
    img_data,
    '{"real_time": %d, "start_time": %d, "htype": "dconfig-1.0", "stop_time": %d}' % (exposure * in_ns, start_time * in_ns, end_time * in_ns)
    ]

  m = hashlib.md5()
  m.update(image_frame[1])
  hash = m.hexdigest()
  image_frame[0] = json.dumps({
      'htype': 'dimage-1.0',
      'series': series,
      'frame': frameid,
      'hash': hash})
  return image_frame

def message_end(series='08/15'):
  return [
    json.dumps({'htype': 'dseries_end-1.0',
                'series': series})
    ]

context = zmq.Context()
# Socket to send messages on
sender = context.socket(zmq.PUSH)
sender.bind("tcp://*:%d" % options.port)

if options.header_full:
  sender.send_multipart(message_header_full())
elif options.header_basic:
  sender.send_multipart(message_header_basic())
else:
  sender.send_multipart(message_header_none())

sys.stdout.write('START ')
sys.stdout.flush()

for task_nbr in xrange(options.numimgs):
  sender.send_multipart(message_image(frameid=task_nbr))
  sys.stdout.write(' %d' % task_nbr)
  sys.stdout.flush()
  time.sleep(options.exposure_time)

sender.send_multipart(message_end())
sys.stdout.write(' END.\n')
sys.stdout.flush()

context.destroy() # optional
