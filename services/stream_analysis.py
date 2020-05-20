import logging
import threading
import time

import confluent_kafka
import dlstbx.util.kafka
import dxtbx.format.FormatEigerStream
import msgpack
import workflows.recipe
from dials.command_line.find_spots_server import work
from six.moves import queue
from workflows.services.common_service import CommonService


class _WorkerThread(threading.Thread):
    """Per-Image-Analysis thread."""

    def __init__(self, dsa_instance):
        threading.Thread.__init__(self)
        self.queue = queue.Queue()
        self._DSA = dsa_instance
        self.log = self._DSA.log
        print("Subthread init")

        # Set up mock image file
        self.mockfile = "/dev/shm/eiger.stream"
        with open(self.mockfile, "w") as fh:
            fh.write("EIGERSTREAM")

    def run(self):
        print("Subthread live")
        while True:
            job = self.queue.get(True)
            print("Gobble")
            # TODO: log extender
            try:
                dcid, rw, header, message = job
                retry = self.stream_analysis(rw, header, message)
            except Exception as e:
                self.log.error(
                    "Uncaught exception in analysis thread: %s", e, exc_info=True
                )
                # TODO: nack?
                continue
            if retry:
                print("put back")
                self.queue.put(job)
                time.sleep(2)
                # TODO: something more intelligent

    def stream_analysis(self, rw, header, message):
        """Run PIA on one image."""

        # Set up PIA parameters
        parameters = rw.recipe_step.get("parameters", None)
        if parameters:
            parameters = ["{k}={v}".format(k=k, v=v) for k, v in parameters.items()]
        else:
            parameters = ["d_max=40"]

        # Obtain the image data
        c = confluent_kafka.Consumer(
            {
                "bootstrap.servers": "ws133",
                "group.id": "mygroup",
                "auto.offset.reset": "error",
                "message.max.bytes": 52428800,
            }
        )
        topic = "hoggery.%d.data" % message["dcid"]
        topic_info = c.list_topics(topic=topic).topics[topic]
        if topic_info.error:
            self.log.info(
                "Topic %s unavailable, retry later (%s)", topic, topic_info.error.str()
            )
            return True
        partitions = topic_info.partitions
        self.log.debug("Topic has %d partitions", len(partitions))
        offset = message["offset"]
        assignment = [
            confluent_kafka.TopicPartition(topic, tp, offset) for tp in partitions
        ]
        c.assign(assignment)
        m = c.consume(1)
        if len(m) != 1:
            self.log.error("Received %d instead of 1 elements", len(m))
            rw.transport.nack(header)
            return
        if m[0].error():
            self.log.info(
                "Error on reading from topic %s offset %d, retry later (%s)",
                topic,
                offset,
                m[0].error().str(),
            )
            return True
        self.log.debug("Received payload")
        mm = msgpack.unpackb(m[0].value(), raw=False, max_bin_len=10 * 1024 * 1024)

        # Do the per-image-analysis
        self.log.info(
            "Running PIA on %d-%d with parameters %s",
            mm[0]["acqID"],
            mm[0]["frame"],
            parameters,
        )
        try:
            dxtbx.format.FormatEigerStream.injected_data = {
                "header2": '{"auto_summation":1,"beam_center_x":1908.5810546875,"beam_center_y":2188.41552734375,"bit_depth_readout":12,"calibration_type":"standard","count_time":0.016656000167131424,"countrate_correction_applied":1,"countrate_correction_bunch_mode":"continuous","data_collection_date":"2016-06-02T10:55:45.424234","description":"Dectris Eiger 16M","detector_distance":0.15001900494098663,"detector_number":"E-32-0100","detector_orientation":[-1,0,0,0,-1,0],"detector_readout_period":0.0075177969411015511,"detector_readout_time":9.9999997473787516e-06,"detector_translation":[0.14314357936382294,0.16413117945194244,-0.15001900494098663],"efficiency_correction_applied":0,"element":"","flatfield_correction_applied":1,"frame_count_time":0.0012720000231638551,"frame_period":0.0012819999828934669,"frame_time":0.016666000708937645,"nframes_sum":13,"nimages":25,"ntrigger":23,"number_of_excluded_pixels":1207641,"photon_energy":12398.4228515625,"pixel_mask_applied":1,"sensor_material":"Si","sensor_thickness":0.00031999999191612005,"software_version":"1.5.2","summation_nimages":1,"threshold_energy":6199.21142578125,"trigger_mode":"exts","virtual_pixel_correction_applied":1,"wavelength":0.99999970197677612,"x_pixel_size":7.5000003562308848e-05,"x_pixels_in_detector":4150,"y_pixel_size":7.5000003562308848e-05,"y_pixels_in_detector":4371}',
                "streamfile_2": mm[1],
                "streamfile_3": mm[2],
            }
            results = work(self.mockfile, cl=parameters)
        except Exception as e:
            self.log.error("PIA failed with %r", e, exc_info=True)
            rw.transport.nack(header)
            return

        # Pass through all file* fields
        for key in (x for x in message if x.startswith("file")):
            results[key] = message[key]
        results["file-number"] = mm[0]["frame"]

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin()
        rw.transport.ack(header, transaction=txn)

        # Send results onwards
        rw.set_default_channel("result")
        rw.send_to("result", results, transaction=txn)
        rw.transport.transaction_commit(txn)
        self.log.info(
            "PIA completed on %d-%d, %d spots found",
            mm[0]["acqID"],
            mm[0]["frame"],
            results["n_spots_total"],
        )


class DLSStreamAnalysis(CommonService):
    """A service that analyses individual images from a stream."""

    # Human readable service name
    _service_name = "DLS Stream-Analysis"

    # Logger name
    _logger_name = "dlstbx.services.stream_analysis"

    def initializing(self):
        """Subscribe to stream activity log on Kafka."""
        logging.getLogger("dials").setLevel(logging.WARNING)

        self.worker = _WorkerThread(dsa_instance=self)
        self.worker.start()
        self.al = dlstbx.util.kafka.ActivityWatcher(callback_new=self.kafka_event_new)
        self.al.start()

    def kafka_event_new(self, dcid):
        """When stream activity is detected on Kafka start listening on relevant queues for commands"""
        self.log.info("Kafka event detected on DCID %i", dcid)

        def p(rw, header, message):
            self.log.info("Received command for DCID %i", dcid)
            self.worker.queue.put((dcid, rw, header, message))

        workflows.recipe.wrap_subscribe(
            self._transport,
            "transient.stream.%i" % dcid,
            p,
            acknowledgement=True,
            log_extender=self.extend_log,
        )
