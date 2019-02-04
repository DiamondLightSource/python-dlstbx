from __future__ import absolute_import, division, print_function

import logging
import sys
from optparse import SUPPRESS_HELP, OptionParser

from confluent_kafka import Producer
import dlstbx
from dlstbx.util.colorstreamhandler import ColorStreamHandler
import msgpack
import zmq

log = logging.getLogger("dlstbx.odin2kafka")


def delivery_report(err, msg):
    """
    Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush().
    """
    if err:
        log.warning("Message delivery failed: {}".format(err))
    else:
        log.info("Message delivered to {} [{}] @{}".format(msg.topic(), msg.partition(), msg.offset()))

if __name__ == "__main__":
    parser = OptionParser(usage="dlstbx.odin2kafka [options]")
    parser.add_option("-?", action="help", help=SUPPRESS_HELP)

    parser.add_option(
        "-k",
        "--kafka",
        dest="kafka",
        default="ws133",
        help="Kafka server to connect to",
    )
    parser.add_option(
        "-z",
        "--zeromq",
        dest="zeromq",
        default="tcp://127.0.0.1:9999",
        help="ZeroMQ stream to connect to",
    )

    (options, args) = parser.parse_args()

    dlstbx.enable_graylog()
    console = ColorStreamHandler()
    console.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(console)
    log.setLevel(logging.DEBUG)

    log.info(
        "Forwarding ZeroMQ stream from %s to Kafka server on %s",
        options.zeromq,
        options.kafka,
    )

    p = Producer({"bootstrap.servers": options.kafka, "message.max.bytes": 52428800})
    log.debug("Kafka connection set up")
    context = zmq.Context()
    # receive work
    consumer_receiver = context.socket(zmq.PULL)
    consumer_receiver.connect(options.zeromq)
    log.debug("ZeroMQ connection set up")

    while True:
        p.poll(0)
        data = consumer_receiver.recv_multipart(copy=True)
        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)

        log.info("Received %d part multipart message (%d bytes)", len(data), sum(len(x) for x in data))
        serial_data = msgpack.packb(data)

        print(data[0])
        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        log.debug("Serialised to %d bytes", len(serial_data))
        p.produce("test", serial_data, callback=delivery_report)
        p.poll(0)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()
