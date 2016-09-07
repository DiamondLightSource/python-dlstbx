from __future__ import division
import sys
import time
import zmq

print("Current libzmq version is %s" % zmq.zmq_version())
print("Current  pyzmq version is %s" % zmq.__version__)

context = zmq.Context()
print("Connecting to 'Eiger'")
receiver = context.socket(zmq.PULL)
# receiver.setsockopt(zmq.RCVBUF, 2) # this may come into play once we throw larger frames about
receiver.connect("tcp://localhost:5557")

for x in range(102):
  s = receiver.recv()
# Simple progress indicator for the viewer

  print s

  time.sleep(0.01)

time.sleep(1)

