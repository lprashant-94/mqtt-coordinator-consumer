
import logging
import random
import sys
import time

import paho.mqtt.client as paho

from utils.constants import BROKER, CONSUMER_CLIENTID, NUMBER_OF_PARTITION
from utils.graceful_killer import GracefulKiller

logger = logging.getLogger(__name__)


class CoordinatedConsumer(object):
    clients = []
    on_message = None

    def __init__(self, client_id, clean_session=False):
        self.client_id = client_id
        self.clean_session = clean_session

    def restart(self):
        self.disconnect()
        self.start()

    def start(self, start_index=None, end_index=None):
        self.start_index = start_index if start_index is not None else self.start_index
        self.end_index = end_index if end_index is not None else self.end_index
        for i in range(self.start_index, self.end_index):
            client = paho.Client(self.client_id + str(i), clean_session=self.clean_session,
                                 userdata={'partition': i, 'consumer': self})
            client.connect(BROKER)
            client.on_message = self.log_message
            client.loop_start()
            self.clients.append(client)

    def subscribe(self, topic, qos=1):
        for client in self.clients:
            topic_p = topic + "/" + str(client._userdata['partition'])
            logger.info("Subscribing to topic: %s", topic_p)
            client.subscribe(topic_p, qos)

    def disconnect(self):
        for client in self.clients:
            logger.info("disconnecting clients %s", client._client_id)
            client.disconnect()
            client.loop_stop()
        self.clients = []

    @staticmethod
    def log_message(client, userdata, message):
        logger.debug("received message =%s", str(message.payload.decode("utf-8")))
        if userdata['consumer'].on_message != None:
            userdata['consumer'].on_message(client, userdata, message)


class CoordinatorManager(object):

    myrandom = 0
    randoms = []
    rebalance_message = "My Random Number: "

    def __init__(self):
        self.consumer = CoordinatedConsumer(CONSUMER_CLIENTID, clean_session=False)
        self.manager_cid = 'MANAGER' + CONSUMER_CLIENTID + str(random.randint(0, 200))
        self.manager_topic = "manager/management/live"

        self.manager = paho.Client(self.manager_cid, userdata={'consumer': self.consumer})

    def start(self):
        self.manager.connect(BROKER)
        self.manager.will_set(self.manager_topic,
                              "I am Unexpectedly Dieing, Please take care %s :-(" % self.manager_cid, qos=1)
        self.manager.on_message = self.rebalance_start
        self.manager.loop_start()
        self.manager.subscribe(self.manager_topic)
        self.manager.publish(self.manager_topic, "I am Live " + self.manager_cid)

    def stop(self):
        self.manager.on_message = None
        self.manager.publish(self.manager_topic, "I am dieing Gracefully %s :-)" % self.manager_cid)
        self.manager.disconnect()
        self.manager.loop_stop()

    @property
    def coordinated_consumer(self):
        return self.consumer

    @property
    def coordinator_manager(self):
        return self.manager

    def rebalance_start(self, client, userdata, message):
        """Rebalance strategy

        First disconnect all consumers.
        Create rebalancing topic. Publish random number on that topic.
        Subscribe to same topic and receive random numbers published by everyone.
        Wait for 5 seconds to receive all numbers. After receving them Sort them in
        ascending order. Check index of your own number. you need to take care of
        that part of partitions. Pass those values for consumer.start function.

        Validatation of number of clients online.
        publish number of responses received on rebalance topic. Also read others count
        on same topic. If count doesn't match, do rebalance again.
        """
        logger.info("received message from manager")
        self.consumer.disconnect()
        time.sleep(1)
        logger.debug("one second after disconnect...")
        self.myrandom = random.randint(0, 100000)
        self.randoms = []

        self.rebalance_topic = "manager/rebalance"
        self.manager.unsubscribe(self.manager_topic)
        self.manager.on_message = self.random_number_acc
        self.manager.subscribe(self.rebalance_topic)

        import threading
        threading._start_new_thread(self.thread_exec, ())
        return 0

    def thread_exec(self):
        """Reassign Partition numbers to consumer.

        wait for 3 seconds to get random number from each client. Sort those
        number and calculate which partitions to assign to current consumer.
        Start current consumer with those assigned partitions.
        """
        logger.info("Negotiation thread started")
        for i in range(6):
            logger.debug("sleeping " + str(i))
            # Keep publishing, so late comers will also get it.
            self.manager.publish(self.rebalance_topic, self.rebalance_message + str(self.myrandom))
            time.sleep(0.5)

        # Add validation here....
        self.manager.unsubscribe(self.rebalance_topic)
        self.manager.subscribe(self.manager_topic)
        self.manager.on_message = self.rebalance_start

        self.randoms = list(set(self.randoms))
        self.randoms.sort()

        logger.info("After waiting for 5 seconds for rebalance messages. ")
        logger.info("My random %d all randoms %r", self.myrandom, self.randoms)

        index = self.randoms.index(self.myrandom)
        count = len(self.randoms)
        batch_size = NUMBER_OF_PARTITION / count
        if NUMBER_OF_PARTITION % count != 0:
            batch_size += 1

        start_index = index * batch_size
        end_index = (index + 1) * batch_size

        if(end_index > NUMBER_OF_PARTITION):  # Last partition, Assign all remaining partitions
            end_index = NUMBER_OF_PARTITION

        logger.info("Consumer starting from %d to %d" % (start_index, end_index))
        # Update topic list according to random numbers here.
        self.consumer.start(start_index, end_index)
        consumer_topic = "house/bulb"
        self.consumer.subscribe(consumer_topic)

    def random_number_acc(self, client, userdata, message):
        logger.info("Received message %s", message.payload)
        if message.payload[:18] == self.rebalance_message:
            self.randoms.append(int(message.payload[18:]))


def on_message(client, userdata, message):
    logger.info("Processing message from clients topic: %s payload: %s",
                message.topic, message.payload)


if __name__ == '__main__':
    killer = GracefulKiller()

    manager = CoordinatorManager()
    manager.start()

    consumer = manager.coordinated_consumer
    consumer.start(0, 0)
    consumer.on_message = on_message
    consumer.subscribe("house/bulb")
    for i in range(100):
        time.sleep(10)
        if killer.kill_now:
            consumer.disconnect()
            break
    manager.stop()
