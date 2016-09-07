import random
import json
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
  start_time = time.time()
  exposure = 0.1
  end_time = start_time + exposure
  in_ns = 1000*1000*1000
  return [
    json.dumps({'htype': 'dimage-1.0',
                'series': series,
                'frame': frameid,
                'hash': 'md5md5md5md5'}),
    json.dumps({'htype': 'dimage_d-1.0',
                'shape': [ 800, 600 ],
                'type': 'uint32',
                'encoding': '<',
                'size': 100}),
    '0'*100*4,
    '{"real_time": %d, "start_time": %d, "htype": "dconfig-1.0", "stop_time": %d}' % (exposure * in_ns, start_time * in_ns, end_time * in_ns)
    ]

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
