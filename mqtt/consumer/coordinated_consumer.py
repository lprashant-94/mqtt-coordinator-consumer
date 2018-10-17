"""Coordinated MQTT consumer."""
import logging
import random
import sys
import threading
import time

import paho.mqtt.client as paho
from utils.constants import NUMBER_OF_PARTITION

is_py2 = sys.version[0] == '2'
if is_py2:
    import Queue as queue  # pylint: disable=import-error
else:
    import queue as queue


logger = logging.getLogger(__name__)
__author__ = "Prashant"


class CoordinatedConsumer(object):
    clients = []
    on_message = None
    topics = []
    batched_messages = queue.Queue()
    start_index = None
    end_index = None

    def __init__(self, client_id, broker, clean_session=False, ):
        self.broker = broker
        self.client_id = client_id
        self.clean_session = clean_session

    def restart(self):
        self.disconnect()
        self.start()

    def start(self, start_index=None, end_index=None):
        if self.clients:
            logger.error("Can't start Consumer, Clients are not disconnected")
            raise AssertionError("Can't start Consumer, Clients are not disconnected")
        self.start_index = start_index if start_index is not None else self.start_index
        self.end_index = end_index if end_index is not None else self.end_index
        for i in range(int(self.start_index), int(self.end_index)):
            client = paho.Client(self.client_id + str(i), clean_session=self.clean_session,
                                 userdata={'partition': i, 'consumer': self})
            client.connect(host=self.broker[0], port=self.broker[1])
            client.on_message = self.log_message
            client.loop_start()
            self.clients.append(client)
        self._subscribe_all()

    def assignment(self):
        return "Subscribing From %r to %r" % (self.start_index, self.end_index)

    def subscribe(self, topic, qos=1):
        self.topics.append({'topic': topic, 'qos': qos})
        self._unsubscribe_all()
        self._subscribe_all()

    def _unsubscribe_all(self):
        for client in self.clients:
            for topic_obj in self.topics:
                topic_p = topic_obj['topic'] + "/" + str(client._userdata['partition'])
                logger.info("Unsubscribing to topic: %s", topic_p)
                client.unsubscribe(topic_p)

    def _subscribe_all(self):
        for client in self.clients:
            for topic_obj in self.topics:
                topic_p = topic_obj['topic'] + "/" + str(client._userdata['partition'])
                logger.info("Subscribing to topic: %s", topic_p)
                client.subscribe(topic_p, topic_obj['qos'])

    def disconnect(self):
        self._unsubscribe_all()
        for client in self.clients:
            logger.info("disconnecting clients %s", client._client_id)
            client.disconnect()
            client.loop_stop()
        self.clients = []
    close = disconnect

    def poll(self, max=5000, timeout=0):
        output = []
        try:
            output.append(self.batched_messages.get(block=True, timeout=timeout))
            for _ in range(max - 1):
                output.append(self.batched_messages.get_nowait())
        except queue.Empty:
            pass
        return output

    @staticmethod
    def log_message(client, userdata, message):
        logger.debug("received message =%s", str(message.payload.decode("utf-8")))
        userdata['consumer'].batched_messages.put(message)
        if userdata['consumer'].on_message is not None:
            userdata['consumer'].on_message(client, userdata, message)
