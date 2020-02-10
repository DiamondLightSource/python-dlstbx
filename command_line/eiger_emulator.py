from __future__ import absolute_import, division, print_function

import hashlib
import json
import os
import random
import sys
import time
from optparse import SUPPRESS_HELP, OptionParser

import fabio
import lz4
import zmq

# EIGER emulator


print("Current libzmq version is %s" % zmq.zmq_version())
print("Current  pyzmq version is %s" % zmq.__version__)


parser = OptionParser()
parser.add_option("-?", help=SUPPRESS_HELP, action="help")
parser.add_option(
    "-f",
    "--file",
    dest="filename",
    help="image file to send",
    metavar="FILE",
    default="/dls/science/users/wra62962/zmq/adsc_streamer/test/testcrystal_1_001.img",
)
parser.add_option(
    "-n",
    "--num-images",
    dest="numimgs",
    type="int",
    help="number of images to send, default 100-130",
    metavar="NUM",
    default=int(random.uniform(100, 130)),
)
parser.add_option(
    "-p",
    "--port",
    dest="port",
    type="int",
    help="TCP port to wait on (%default)",
    metavar="PORT",
    default=5557,
)
parser.add_option(
    "--basic",
    action="store_true",
    dest="header_basic",
    default=False,
    help="Generate basic detail stream headers",
)
parser.add_option(
    "--full",
    action="store_true",
    dest="header_full",
    default=False,
    help="Generate full detail stream headers (unsupported)",
)
parser.add_option(
    "--abort",
    action="store_true",
    dest="abort",
    default=False,
    help="Abort data collection mid-run",
)
parser.add_option(
    "--exposure-time",
    dest="exposure_time",
    type="float",
    default=0,
    help="Waiting (exposure) time between images",
)

# image header parameters
options, args = parser.parse_args()

if options.exposure_time < 1 / 133:
    options.exposure_time = 1 / 133

if not os.path.exists(options.filename):
    print("Use -f to point to an image file. Use --help to see command line options")
    sys.exit(1)

# Read image data
img_file = fabio.open(options.filename)
img_header = img_file.header
img_data = lz4.dumps(img_file.data)


def message_header_none(series="08/15"):
    return [
        json.dumps({"htype": "dheader-1.0", "series": series, "header_detail": "none"})
    ]


def message_header_basic(series="08/15"):
    return [
        json.dumps(
            {"htype": "dheader-1.0", "series": series, "header_detail": "basic"}
        ),
        json.dumps(
            {
                "auto_summation": True,
                "beam_center_x": random.uniform(200, 240),
                "beam_center_y": random.uniform(200, 240),
                "bit_depth_image": 16,
                "bit_depth_readout": 16,
                "chi_increment": None,
                "chi_start": None,
                "compression": "lz4",
                "count_time": options.exposure_time - 0.000001,
                "countrate_correction_applied": True,
                "countrate_correction_count_cutoff": 65535,
                "data_collection_date": "Thu Sep  8 13:37:18 BST 2016",
                "description": "EIGER 16M simulator",
                "detector_distance": 160,
                "detector_number": 1,
                "detector_readout_time": 0.000001,  # value? unit?
                "element": "Mo",
                "flatfield": [1, 1],
                "flatfield_correction_applied": True,
                "frame_time": options.exposure_time,
                "kappa_increment": None,
                "kappa_start": None,
                "nimages": options.numimgs,
                "ntrigger": None,
                "number_of_excluded_pixels": 0,
                "omega_increment": None,
                "omega_start": None,
                "phi_increment": 0.1,
                "phi_start": 0.0,
                "photon_energy": 18000,  # unit?
                "pixel_mask": None,
                "pixel_mask_applied": True,
                "roi_mode": "16M",
                "sensor_material": "Si",
                "sensor_thickness": 0.270,
                "software_version": "Thursday",
                "threshold_energy": 12000,
                "trigger_mode": "happy",
                "two_theta_increment": None,
                "two_theta_start": None,
                "wavelength": 0.68890,
                "x_pixel_size": 0.016,
                "x_pixels_in_detector": int(img_header["SIZE1"]),
                "y_pixel_size": 0.016,
                "y_pixels_in_detector": int(img_header["SIZE2"]),
            }
        ),
    ]


def message_image(series="08/15", frameid=0, start_time=0.0):
    exposure = options.exposure_time
    end_time = start_time + exposure
    in_ns = 1000 * 1000 * 1000

    image_frame = [
        None,
        json.dumps(
            {
                "htype": "dimage_d-1.0",
                "shape": [int(img_header["SIZE1"]), int(img_header["SIZE2"])],
                "type": "uint16",
                "encoding": "lz4<",
                "size": len(img_data),
            }
        ),
        img_data,
        '{"real_time": %d, "start_time": %d, "htype": "dconfig-1.0", "stop_time": %d}'
        % (exposure * in_ns, start_time * in_ns, end_time * in_ns),
    ]

    m = hashlib.md5()
    m.update(image_frame[1])
    hash = m.hexdigest()
    image_frame[0] = json.dumps(
        {"htype": "dimage-1.0", "series": series, "frame": frameid, "hash": hash}
    )
    return image_frame


def message_end(series="08/15"):
    return [json.dumps({"htype": "dseries_end-1.0", "series": series})]


context = zmq.Context()
# Socket to send messages on
sender = context.socket(zmq.PUSH)
sender.bind("tcp://*:%d" % options.port)

if options.header_full:
    raise NotImplementedError("full header implementation missing")
    # sender.send_multipart(message_header_full())
elif options.header_basic:
    sender.send_multipart(message_header_basic())
else:
    sender.send_multipart(message_header_none())

sys.stdout.write("START ")
sys.stdout.flush()
start_time = time.time()
image_time = start_time

for task_nbr in range(options.numimgs):
    sender.send_multipart(message_image(frameid=task_nbr, start_time=image_time))
    sys.stdout.write(" %d" % task_nbr)
    if options.abort and task_nbr > random.uniform(30, 70):
        sys.stdout.write(" ABORT")
        sys.stdout.flush()
        break
    sys.stdout.flush()

    current_time = time.time()
    image_time = image_time + options.exposure_time
    if current_time <= image_time:
        time.sleep(image_time - current_time)

sender.send_multipart(message_end())
sys.stdout.write(" END.\n")
sys.stdout.flush()

context.destroy()  # optional
