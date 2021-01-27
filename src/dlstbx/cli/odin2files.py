# Read an odin 0mq stream and dump it to files in the visit tmp directory

import json
import logging
import re
from optparse import SUPPRESS_HELP, OptionParser

import dlstbx
import ispyb
import msgpack
import py
import zmq
from dlstbx.util.colorstreamhandler import ColorStreamHandler

log = logging.getLogger("dlstbx.odin2files")


def run():
    parser = OptionParser(usage="dlstbx.odin2files [options]")
    parser.add_option("-?", action="help", help=SUPPRESS_HELP)

    parser.add_option(
        "-z",
        "--zeromq",
        dest="zeromq",
        default="tcp://127.0.0.1:9999",
        help="ZeroMQ stream to connect to (default: %default)\nI03: tcp://cs04r-sc-serv-22:9009",
    )
    (options, args) = parser.parse_args()
    dlstbx.enable_graylog()
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

    last_dcid = None
    last_destination = None
    re_visit_base = re.compile(r"^(.*\/[a-z][a-z][0-9]+-[0-9]+)\/")
    try:
        while True:
            try:
                data = consumer_receiver.recv_multipart(copy=True)
                header = data[0] = json.loads(data[0])
                if not header.get("acqID"):
                    log.error(
                        "Received multipart message without DCID with sizes %r and content:\n%r",
                        [len(x) for x in data],
                        header,
                    )
                    continue
                dcid = int(header["acqID"])
                if dcid == last_dcid:
                    destination = last_destination
                else:
                    image_directory = i.get_data_collection(dcid).file_directory
                    visit_base = re_visit_base.search(image_directory)
                    if not visit_base:
                        log.error(
                            "Could not find visit base directory for DCID %r, file directory %r",
                            dcid,
                            image_directory,
                        )
                        continue
                    destination = (
                        py.path.local(visit_base.group(1)) / "tmp" / "dump" / str(dcid)
                    )
                    last_dcid = dcid
                    last_destination = destination
                image_number = header.get("frame")
                if image_number is not None:
                    destination_file = "image%06d" % image_number
                elif header.get("htype") == "dheader-1.0":
                    destination_file = "header"
                elif header.get("htype") == "dseries_end-1.0":
                    destination_file = "end"
                else:
                    log.error(
                        "Received undecypherable multipart message with sizes %r and content:\n%r",
                        [len(x) for x in data],
                        header,
                    )
                    continue
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
            except Exception:
                log.error("Unhandled exception in odin2files", exc_info=True)
    except KeyboardInterrupt:
        pass
