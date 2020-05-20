from confluent_kafka import Producer
import msgpack
import zmq


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


p = Producer({"bootstrap.servers": "ws133", "message.max.bytes": 52428800})
context = zmq.Context()
# receive work
consumer_receiver = context.socket(zmq.PULL)
consumer_receiver.connect("tcp://127.0.0.1:9999")

while True:
    p.poll(0)
    data = consumer_receiver.recv_multipart()
    # Trigger any available delivery report callbacks from previous produce() calls
    print("Received %d part multipart message" % len(data))
    p.poll(0)
    data = "msg" + msgpack.packb(data, use_bin_type=True)
    p.poll(0)

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    print("Sending on %d bytes" % len(data))
    p.produce("test", data, callback=delivery_report)
    p.poll(0)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
