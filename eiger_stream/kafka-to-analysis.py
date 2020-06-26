import json
import os
import sys

import msgpack
from confluent_kafka import Consumer, KafkaError
from dials.command_line.find_spots_server import work
import dxtbx.format.FormatEigerStream
from pprint import pprint

sys.stdout = os.fdopen(sys.stdout.fileno(), "w", 0)

c = Consumer(
    {
        "bootstrap.servers": "ws133",
        "group.id": "mygroup",
        "auto.offset.reset": "earliest",
        "message.max.bytes": 52428800,
    }
)

c.subscribe(["test"])
assembled_data = {}
dxtbx.format.FormatEigerStream.injected_data = assembled_data

filename = "eiger.stream"
try:
    with open(filename, "w") as fh:
        fh.write("EIGERSTREAM")
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        if msg.value()[0:3] == "msg":
            print("Messagepack detected")
            mm = msgpack.unpackb(msg.value()[3:], raw=False, strict_map_key=False)
            for n, msg in enumerate(mm):
                print(
                    "Received multipack message {} with {:7} bytes: {}".format(
                        n, len(msg), msg[0:10]
                    )
                )
            header = json.loads(mm[0])
            print(header.get("htype"))
            if header.get("htype").startswith("dheader-"):
                assembled_data["header1"] = mm[0]
                assembled_data["header2"] = mm[1]
                pprint(mm)
                continue
            if header.get("htype").startswith("dimage-"):
                if "header2" not in assembled_data:
                    print("NO header seen yet :(")
                    continue
                assembled_data["streamfile_1"] = mm[0]
                assembled_data["streamfile_2"] = mm[1]
                assembled_data["streamfile_3"] = mm[2]
                assembled_data["streamfile_4"] = mm[3]
                parameters = ["d_max=40"]
                results = work(filename, cl=parameters)
                pprint(results)
                continue
            assert False
        else:
            print(":(")
except KeyboardInterrupt:
    pass
finally:
    os.remove(filename)
    c.close()
