from __future__ import absolute_import, division, print_function

import json
import threading
import time

import ispyb
import msgpack
from workflows.services.common_service import CommonService


class ZMQReceiver(threading.Thread):
    def __init__(self, logger, zmq_stream):
        super(ZMQReceiver, self).__init__()
        self.dcid_cache = {}
        self.log = logger
        self.zmq_stream = zmq_stream

    def shutdown(self):
        self.closing = True

    def run(self):
        """Connect to the ZeroMQ stream."""
        self.closing = False
        self.log.info("Connecting to ISPyB")
        with ispyb.open("/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg") as i:
            self.log.debug("ISPyB connection set up")

            self.log.info("Connecting to ZeroMQ stream at %s", self.zmq_stream)
            self.zmq_context = zmq.Context()
            self.zmq_consumer = self.zmq_context.socket(zmq.PULL)
            self.zmq_consumer.connect(self.zmq_stream)

            re_visit_base = re.compile("^(.*\/[a-z][a-z][0-9]+-[0-9]+)\/")
            try:
                while not self.closing:
                    try:
                        data = consumer_receiver.recv_multipart(copy=True)
                        header = data[0] = json.loads(data[0])
                        if not header.get("acqID"):
                            self.log.error(
                                "Received multipart message without DCID with sizes %r and content:\n%r",
                                [len(x) for x in data],
                                header,
                            )
                            continue
                        dcid = int(header["acqID"])
                        destination = self.dcid_cache.get(dcid)
                        if not destination:
                            image_directory = i.get_data_collection(dcid).file_directory
                            visit_base = re_visit_base.search(image_directory)
                            if not visit_base:
                                self.log.error(
                                    "Could not find visit base directory for DCID %r, file directory %r",
                                    dcid,
                                    image_directory,
                                )
                                continue
                            self.dcid_cache[dcid] = destination = (
                                py.path.local(visit_base.group(1))
                                / "tmp"
                                / "dump"
                                / str(dcid)
                            )
                        image_number = header.get("frame")
                        if image_number is not None:
                            destination_file = "image%06d" % image_number
                        elif header.get("htype") == "dheader-1.0":
                            destination_file = "header"
                        elif header.get("htype") == "dseries_end-1.0":
                            destination_file = "end"
                        else:
                            self.log.error(
                                "Received undecypherable multipart message with sizes %r and content:\n%r",
                                [len(x) for x in data],
                                header,
                            )
                            continue
                        self.log.info(
                            "Received %d part multipart message for %s (%d bytes)",
                            len(data),
                            destination_file,
                            sum(len(x) for x in data),
                        )
                        serial_data = msgpack.packb(data, use_bin_type=True)
                        self.log.debug("Serialised to %d bytes", len(serial_data))
                        target_file = destination.join(destination_file)
                        self.log.debug("Writing to %s", target_file.strpath)
                        target_file.write_binary(serial_data, ensure=True)
                        self.log.info("Done")
                    except Exception as e:
                        self.log.error("Unhandled exception %r", e, exc_info=True)
            except KeyboardInterrupt:
                pass


class DLSStreamdumper(object):
    """A service that triggers actions running on stream data."""

    # Logger name
    _logger_name = "dlstbx.services.streamdumper"

    def initializing(self):
        """Start ZMQ listener thread."""
        self.dumper_thread = ZMQReceiver(self.log, self.zmq_stream)
        self.dumper_thread.daemon = True
        self.dumper_thread.name = "ZMQ"
        self.dumper_thread.start()


class DLSStramdumperI03(CommonService, DLSStreamdumper):
    # Human readable service name
    _service_name = "DLS Streamdumper (I03)"

    # ZeroMQ endpoint
    zmq_stream = "tcp://cs04r-sc-serv-22:9009"
