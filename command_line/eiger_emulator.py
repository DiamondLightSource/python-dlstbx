import fabio
import hashlib
import json
import lz4
import os
import random
import sys
import time
import zmq

print("Current libzmq version is %s" % zmq.zmq_version())
print("Current  pyzmq version is %s" % zmq.__version__)

# EIGER emulator

def message_header_none(series='08/15'):
  return [
    json.dumps({'htype':'dheader-1.0',
                'series': series,
                'header_data': 'none'})
    ]

def message_image(series='08/15', frameid=0):
  filename = '/dls/science/users/wra62962/zmq/adsc_streamer/test/testcrystal_1_001.img'

  james = '/home/upc86898/Desktop/EigerStream/zmq/adsc_streamer/test/testcrystal_1_001.img'
  if os.path.exists(james):
    filename = james

  file = fabio.open(filename)
  header = file.header
  data = lz4.dumps(file.data)

  start_time = time.time()
  exposure = 0.1
  end_time = start_time + exposure
  in_ns = 1000*1000*1000

  image_frame = [
    None,
    json.dumps({'htype': 'dimage_d-1.0',
                'shape': [int(header['SIZE1']), int(header['SIZE2'])],
                'type': 'uint16',
                'encoding': 'adsc',
                'size': len(data)}),
    'DATA', # data,
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
sender.bind("tcp://*:5557")

sender.send_multipart(message_header_none())

for task_nbr in xrange(100):
  sender.send_multipart(message_image(frameid=task_nbr))
  sys.stdout.write('%3d' % task_nbr)
  sys.stdout.flush()
  time.sleep(1 / 133) # 133 Hz

sender.send_multipart(message_end())

context.destroy() # optional
