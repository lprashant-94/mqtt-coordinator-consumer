import hashlib
import logging
import random
import time
import uuid

import paho.mqtt.client as paho
from utils.constants import NUMBER_OF_PARTITION

logger = logging.getLogger(__name__)


class CoordinatedProducer(object):

    def __init__(self, host, port=1883):
        self.client_id = str(uuid.uuid4())
        self.client = paho.Client(self.client_id)

        self.client.connect(host, port=port)
        self.client.loop_start()

    def publish_on_partition(self, topic, message, partition=None, partition_key=None):
        if partition is None:
            if partition_key is not None:
                partition = int(hashlib.sha1(partition_key).hexdigest(), 16) % NUMBER_OF_PARTITION
            else:
                partition = random.randint(0, NUMBER_OF_PARTITION - 1)
        logger.info("Publishing %s on topic %s, partition %d", message, topic, partition)
        info = self.client.publish(topic + '/' + str(partition), message, qos=1)
        logger.info(info)


if __name__ == '__main__':
    pass
