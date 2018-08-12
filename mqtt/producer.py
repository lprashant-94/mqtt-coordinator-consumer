import hashlib
import random
import time
import uuid

import paho.mqtt.client as paho

from utils.constants import NUMBER_OF_PARTITION


class CoordinatedProducer(object):

    def __init__(self, host, port=1883):
        self.client_id = str(uuid.uuid4())
        self.client = paho.Client(self.client_id)

        self.client.connect(host, port=port)

    def publish_on_partition(self, topic, message, partition=None, partition_key=None):
        if partition is None:
            if partition_key is not None:
                partition = int(hashlib.sha1(partition_key).hexdigest(), 16) % NUMBER_OF_PARTITION
            else:
                partition = random.randint(0, NUMBER_OF_PARTITION - 1)

        self.client.publish(topic + '/' + str(partition), message, qos=1)


if __name__ == '__main__':
    pass
