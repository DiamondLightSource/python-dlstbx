from __future__ import absolute_import, division, print_function

import logging
import threading
import time
import uuid
from pprint import pprint

import confluent_kafka
import msgpack

log = logging.getLogger("dlstbx.util.kafka")


class ActivityWatcher(threading.Thread):
    def __init__(self, callback_new=None, from_timestamp=None):
        threading.Thread.__init__(self)
        self.daemon = True
        self._stop = False
        self._lock = threading.Lock()
        self._status = {}
        self._callback_new = callback_new
        self._from_timestamp = from_timestamp

    def stop(self):
        self._stop = True

    def run(self):
        c = confluent_kafka.Consumer(
            {
                "bootstrap.servers": "ws133",
                "group.id": str(uuid.uuid4()),
                "auto.offset.reset": "earliest",
            }
        )

        def set_start_offset(consumer, partitions):
            if not self._from_timestamp:
                return
            print(partitions)
            for p in partitions:
                p.offset = self._from_timestamp * 1000
            print(partitions)
            offs = c.offsets_for_times(partitions)
            print(offs)
            consumer.assign(offs)

        c.subscribe(["hoggery.activity"], on_assign=set_start_offset)

        while not self._stop:
            try:
                msg = c.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                        continue
                    else:
                        log.error(msg.error())
                        break

                try:
                    update = msgpack.unpackb(msg.value(), raw=False)
                except TypeError:
                    log.warning(
                        "Received invalid message:\n%s",
                        repr(msg.value()),
                        exc_info=True,
                    )
                    continue
                try:
                    dcid = update.get("DCID")
                except AttributeError:
                    log.warning(
                        "Unpacked message is not a dictionary:\n%s",
                        repr(update),
                        exc_info=True,
                    )
                    continue
                try:
                    dcid = int(dcid)
                except ValueError:
                    log.warning(
                        "Invalid DCID in message:\n%s", repr(dcid), exc_info=True
                    )
                    continue
                del update["DCID"]
                # print("Received message:")
                # pprint(update)
                if not update.get("timestamp"):
                    update["timestamp"] = time.time()
                with self._lock:
                    new = dcid not in self._status
                    self._status[dcid] = update
                    # pprint(self._status)
                if new and self._callback_new:
                    self._callback_new(dcid)
            except KeyboardInterrupt:
                self._stop = True
            except Exception as e:
                log.error("Unhandled Exception: " + str(e), exc_info=True)

        c.close()

    def get_status(self, dcid):
        with self._lock:
            return self._status.get(dcid)
