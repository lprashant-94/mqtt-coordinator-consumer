
import logging
import sys
import time

import paho.mqtt.client as paho

from utils.constants import BROKER, CONSUMER_CLIENTID, NUMBER_OF_PARTITION
from utils.graceful_killer import GracefulKiller

logger = logging.getLogger(__name__)


class CoordinatedConsumer(object):
    clients = []
    on_message = None

    def __init__(self, client_id, start, end, clean_session):
        for i in range(start, end):
            client = paho.Client(client_id + str(i), clean_session=False,
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

    @staticmethod
    def log_message(client, userdata, message):
        logger.debug("received message =%s", str(message.payload.decode("utf-8")))
        if userdata['consumer'].on_message != None:
            userdata['consumer'].on_message(client, userdata, message)


if __name__ == '__main__':
    killer = GracefulKiller()

    from_p = int(sys.argv[1])
    to_p = int(sys.argv[2])

    consumer = CoordinatedConsumer(CONSUMER_CLIENTID, from_p, to_p, clean_session=False)

    def on_message(client, userdata, message):
        logger.info("Processing message from clients topic: %s payload: %s",
                    message.topic, message.payload)

    consumer.on_message = on_message
    consumer.subscribe("house/bulb")
    for i in range(100):
        time.sleep(10)
        if killer.kill_now:
            consumer.disconnect()
            break
