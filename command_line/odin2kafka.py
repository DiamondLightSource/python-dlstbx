from __future__ import absolute_import, division, print_function

import json
import logging
import random
import string
import sys
from optparse import SUPPRESS_HELP, OptionParser

import dlstbx
import msgpack
import zmq
from confluent_kafka import Producer
from dlstbx.util.colorstreamhandler import ColorStreamHandler

log = logging.getLogger("dlstbx.odin2kafka")

o2k_instance = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(10))
o2k_index = 0
o2k_map = {}

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

        header = data[0] = json.loads(data[0])
        if header.get('htype') in ('dheader-1.0', 'dseries_end-1.0'):
          target_topic = "hoggery.header"
        else:
          target_topic = "hoggery.data"
        series = header.get('series')
        if series:
          if series not in o2k_map:
            o2k_map[series] = str(o2k_index)
            o2k_index = o2k_index + 1
          series_map = o2k_map[series]
          header['hoggery-id'] = o2k_instance + ":" + series_map + ":" + options.zeromq + ":" + str(series)

        log.info("Received %d part multipart message for %s (%d bytes)", len(data), target_topic, sum(len(x) for x in data))
        print(header)
        serial_data = msgpack.packb(data, use_bin_type=True)
        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        log.debug("Serialised to %d bytes", len(serial_data))
        p.produce(target_topic, serial_data, callback=delivery_report)
        p.poll(0)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()
