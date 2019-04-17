from __future__ import absolute_import, division, print_function

# Read an odin 0mq stream and dump it to files in the visit tmp directory

import json
import logging
import re
from optparse import SUPPRESS_HELP, OptionParser
from pprint import pprint

import dlstbx
import ispyb
import msgpack
import py
import zmq
from dlstbx.util.colorstreamhandler import ColorStreamHandler

log = logging.getLogger("dlstbx.odin2files")


if __name__ == "__main__":
    parser = OptionParser(usage="dlstbx.odin2files [options]")
    parser.add_option("-?", action="help", help=SUPPRESS_HELP)

    parser.add_option(
        "-z",
        "--zeromq",
        dest="zeromq",
        default="tcp://127.0.0.1:9999",
        help="ZeroMQ stream to connect to (default: %default)",
    )
    parser.add_option(
        "--dcid",
        dest="dcid",
        default=None,
        help="Set data collection ID if not defined",
        type=int,
    )
    parser.add_option(
        "--dcid-override",
        dest="override",
        default=False,
        action="store_true",
        help="Override any given data collection ID",
    )
    parser.add_option(
        "--dest-override",
        dest="destoverride",
        default=None,
        help="Override destination path",
    )

    (options, args) = parser.parse_args()
    #   dlstbx.enable_graylog()
    console = ColorStreamHandler()
    console.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(console)
    log.setLevel(logging.DEBUG)

    log.info("Connecting to ISPyB")
    i = ispyb.open("/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg")
    log.debug("ISPyB connection set up")

    log.info("Connecting to ZeroMQ stream from %s", options.zeromq)

    context = zmq.Context()
    # receive work
    consumer_receiver = context.socket(zmq.PULL)
    consumer_receiver.connect(options.zeromq)
    log.debug("ZeroMQ connection set up")

    image_number = 0
    try:
        while True:
            data = consumer_receiver.recv_multipart(copy=True)
            header = data[0] = json.loads(data[0])
            pprint(header)
            if options.override:
                header["acqID"] = options.dcid
            elif not header.get("acqID"):
                header["acqID"] = options.dcid or header.get("series", 1)
            dcid = int(header["acqID"])
            destination = None
            if dcid:
                image_directory = i.get_data_collection(dcid).file_directory
                visit_base = re.search(
                    "^(.*\/[a-z][a-z][0-9]+-[0-9]+)\/", image_directory
                )
                if visit_base:
                    destination = py.path.local(visit_base.group(1)) / "tmp" / "dump" / str(dcid)
            if not destination:
                destination = py.path.local("/dls/tmp/streamalysis")
            if options.destoverride:
                destination = py.path.local(options.destoverride)
            log.info("Writing to %s", destination.strpath)
            if header.get("htype") == "dheader-1.0":
                destination_file = "header"
                image_number = 0
            elif header.get("htype") == "dseries_end-1.0":
                destination_file = "end"
            else:
                image_number = image_number + 1
                destination_file = "image%06d" % image_number
            log.info(
                "Received %d part multipart message for %s (%d bytes)",
                len(data),
                destination_file,
                sum(len(x) for x in data),
            )
            serial_data = msgpack.packb(data, use_bin_type=True)
            log.debug("Serialised to %d bytes", len(serial_data))
            target_file = destination.join(destination_file)
            log.debug("Writing to %s", target_file.strpath)
            target_file.write_binary(serial_data, ensure=True)
            log.info("Done")
    except KeyboardInterrupt:
        pass
