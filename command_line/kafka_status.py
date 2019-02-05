from __future__ import absolute_import, division, print_function

import confluent_kafka

k = confluent_kafka.Consumer({"bootstrap.servers": "ws133"})

for topic, topicmeta in sorted(k.list_topics().topics.items()):
    print(topic)
    for partition in topicmeta.partitions:
        start, end = k.get_watermark_offsets(confluent_kafka.TopicPartition(topic, partition, confluent_kafka.OFFSET_END))
        print("    partition {partition}:  {start}-{end}".format(partition=partition,start=start,end=end))
    print()

