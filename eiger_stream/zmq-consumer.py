from __future__ import absolute_import, division, print_function

import zmq

def consumer():
    context = zmq.Context()
    # receive work
    consumer_receiver = context.socket(zmq.PULL)
    consumer_receiver.connect("tcp://127.0.0.1:9999")

    while True:
        work = consumer_receiver.recv()
        print("Received %d bytes" % len(work))

consumer()
