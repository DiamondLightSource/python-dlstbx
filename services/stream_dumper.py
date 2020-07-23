import json
import re
import threading

import ispyb
import msgpack
import py
from workflows.services.common_service import CommonService
import zmq

# import pysnooper


class ZMQReceiver(threading.Thread):
    def __init__(self, logger, zmq_stream, service_termination_call):
        super().__init__()
        self.log = logger
        self.zmq_stream = zmq_stream
        self._terminate_service = service_termination_call

    # @pysnooper.snoop()
    def shutdown(self):
        self.closing = True

        self.zmq_context.destroy(linger=0)
        # This may crash the *process* with "Resource temporarily unavailable"
        # Shame, https://i.kym-cdn.com/photos/images/original/000/956/638/5bc.gif
        # but nothing we can do about it.

        self.join()

    # @pysnooper.snoop()
    def run(self):
        """Connect to the ZeroMQ stream."""
        self.closing = False

        self._ispyb = None
        try:
            self._ispyb = ispyb.open(
                "/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg"
            )
            self.log.debug("ISPyB connection set up")

            self.log.info("Connecting to ZeroMQ stream at %s", self.zmq_stream)
            self.zmq_context = zmq.Context()
            self.zmq_consumer = self.zmq_context.socket(zmq.PULL)
            self.zmq_consumer.connect(self.zmq_stream)

            self._receiver_loop()
        except Exception as e:
            self.log.error("Unhandled exception %r", e, exc_info=True)
            self._terminate_service()
        finally:
            if self._ispyb:
                self._ispyb.disconnect()
                self._ispyb = None
            if self.zmq_context and not self.zmq_context.closed:
                self.zmq_context.destroy()
            self.zmq_context = None

    def _receiver_loop(self):
        re_visit_base = re.compile(r"^(.*\/[a-z][a-z][0-9]+-[0-9]+)\/")
        dcid_cache = {}
        while not self.closing:
            try:
                data = self.zmq_consumer.recv_multipart(copy=True)
            except zmq.ContextTerminated:
                self.log.info("Context terminated. Shutting down receiver thread")
                self.closing = True
                return
            header = data[0] = json.loads(data[0])
            if not header.get("acqID"):
                self.log.error(
                    "Received multipart message without DCID with sizes %r and content:\n%r",
                    [len(x) for x in data],
                    header,
                )
                continue
            if not str(header["acqID"]).isdigit():
                self.log.error(
                    "Dropped packet with non-numeric DCID %r", header["acqID"]
                )
                continue
            dcid = int(header["acqID"])
            destination = dcid_cache.get(dcid)
            if not destination:
                self.log.debug("DCID %d seen for the first time. Checking ISPyB", dcid)
                try:
                    image_directory = self._ispyb.get_data_collection(
                        dcid
                    ).file_directory
                except Exception:
                    self.log.error(
                        "Could not read information from ISPyB for DCID %d. Packet dropped.",
                        dcid,
                        exc_info=True,
                    )
                    continue
                visit_base = re_visit_base.search(image_directory)
                if not visit_base:
                    self.log.error(
                        "Could not find visit base directory for DCID %r, file directory %r",
                        dcid,
                        image_directory,
                    )
                    continue
                dcid_cache[dcid] = destination = (
                    py.path.local(visit_base.group(1)) / "tmp" / "dump" / str(dcid)
                )
                self.log.debug("DCID %d has destination %s", dcid, destination.strpath)
            image_number = header.get("frame")
            if image_number is not None:
                destination_file = "image%06d" % (image_number + 1)
            elif header.get("htype") == "dheader-1.0":
                destination_file = "header"
                try:
                    data[3] = ""
                    data[5] = ""
                    data[7] = ""
                except IndexError:
                    self.log.warning(
                        "Received header packet with detail level other than 'all'"
                    )
            elif header.get("htype") == "dseries_end-1.0":
                destination_file = "end"
            else:
                self.log.error(
                    "Received undecypherable multipart message with sizes %r and content:\n%r",
                    [len(x) for x in data],
                    header,
                )
                continue
            serial_data = msgpack.packb(data, use_bin_type=True)
            target_file = destination.join(destination_file)
            target_file.write_binary(serial_data, ensure=True)
            self.log.info(
                "Written %d part multipart message for %s to %s (%d bytes)",
                len(data),
                destination_file,
                target_file.strpath,
                len(serial_data),
            )
        self.log.info("Receiver thread terminating")


class DLSStreamdumper:
    """A service that writes ZeroMQ stream messages to files."""

    _logger_name = "dlstbx.services.streamdumper"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dumper_thread = None

    def initializing(self):
        """Start ZMQ listener thread."""
        self.dumper_thread = ZMQReceiver(
            self.log, self._zmq_stream, self._request_termination
        )
        self.dumper_thread.daemon = True
        self.dumper_thread.name = "ZMQ"
        self.log.debug("Starting ZMQ listener thread")
        self.dumper_thread.start()
        # self._register_idle(0.1, self.countdown)

    # @pysnooper.snoop()
    # def countdown(self):
    #     import random
    #
    #     time.sleep(random.random())
    #     self._shutdown()

    # @pysnooper.snoop()
    def in_shutdown(self):
        if not self.dumper_thread:
            return
        self.dumper_thread.shutdown()


class DLSStreamdumperI022(DLSStreamdumper, CommonService):
    _service_name = "DLS Streamdumper (VMXi)"
    _zmq_stream = "tcp://cs04r-sc-serv-116:9009"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DLSStreamdumperI03(DLSStreamdumper, CommonService):
    _service_name = "DLS Streamdumper (I03)"
    _zmq_stream = "tcp://cs04r-sc-serv-22:9009"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DLSStreamdumperI04(DLSStreamdumper, CommonService):
    _service_name = "DLS Streamdumper (I04)"
    _zmq_stream = "tcp://cs04r-sc-serv-23:9009"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DLSStreamdumperI24(DLSStreamdumper, CommonService):
    _service_name = "DLS Streamdumper (I24)"
    _zmq_stream = "tcp://cs04r-sc-serv-24:9009"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DLSStreamdumperTest(DLSStreamdumper, CommonService):
    _service_name = "DLS Streamdumper (Test)"
    _zmq_stream = "tcp://ws154:9999"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
