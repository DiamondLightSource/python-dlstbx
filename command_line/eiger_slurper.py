from __future__ import division
import json
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

s = receiver.recv_multipart()
img_expected = None
if s and s[0] and 'dheader-1.0' in s[0]:
  header = json.loads(s[0])
  if 'header_detail' in header:
    print "Receiving data stream, '%s' format:" % header['header_detail']
    if header['header_detail'] in ('basic', 'all'):
      assert len(s) >= 2
      print s[1]
      img_header = json.loads(s[1])
      img_expected = img_header['nimages']
  else:
    print "ERR: First message is misformatted header"
else:
  print "ERR: First message is not header"

x = 0
while True:
  s = receiver.recv_multipart()

  if s and s[0] and 'dseries_end-1.0' in s[0]:
    print "END."
    if img_expected and x != img_expected:
      print "ERR: %d images expected, but %d received" % (img_expected, x)
    break

  img_header = {}
  if s and len(s) >= 3 and 'dimage-1.0' in s[0]:
    s[2] = '<cut>'
    img_header = json.loads(s[0])
  if img_header and 'frame' in img_header and img_header['frame'] == x:
    sys.stdout.write('.')
    sys.stdout.flush()
  else:
    print "ERR: Expected frame %d, received" % x,
    if img_header:
      if 'frame' in img_header:
        print "frame %d instead." % img_header['frame']
      else:
        print img_header
    else:
      print "something else"

  time.sleep(0.03)
  x = x + 1
  if img_expected and x > img_expected:
    print "ERR: More images (%d) than expected (%d)" % (x, img_expected)

time.sleep(1)
context.destroy()
