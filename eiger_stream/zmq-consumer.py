from __future__ import absolute_import, division, print_function

import zmq


def consumer():
    context = zmq.Context()
    # receive work
    consumer_receiver = context.socket(zmq.PULL)
    consumer_receiver.connect("tcp://127.0.0.1:9999")

    if False:  # Receive messages parts individually
        while True:
            work = consumer_receiver.recv()
            print("Received %d bytes" % len(work))
            if len(work) < 1000:
                print(work)

    while True:
        # Multipart message receiver
        work = consumer_receiver.recv_multipart()
        print(
            "Received {} parts of lengths: {}".format(len(work), list(map(len, work)))
        )


consumer()
