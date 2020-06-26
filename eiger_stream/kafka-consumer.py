import os
import sys

import msgpack
from confluent_kafka import Consumer, KafkaError

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
    else:
        print("Received {:7} bytes: {}".format(len(msg.value()), msg.value()[0:10]))

c.close()
