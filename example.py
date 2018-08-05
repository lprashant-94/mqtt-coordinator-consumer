import logging
import threading
import time

from mqtt import CoordinatedProducer, CoordinatorManager
from utils.constants import TEST_TOPIC
from utils.graceful_killer import GracefulKiller

logger = logging.getLogger(__name__)


def producer():
    coordinated_producer = CoordinatedProducer()
    for i in range(0, 10):
        logger.info("publishing on " + str(i))
        coordinated_producer.publish_on_partition("house/bulb/", "on" + str(i))
        time.sleep(0.01)


def on_message(client, userdata, message):
    logger.info("Processing message from clients topic: %s payload: %s",
                message.topic, message.payload)


def consumer():
    manager = CoordinatorManager()
    manager.start()

    consumer = manager.coordinated_consumer
    consumer.on_message = on_message
    consumer.subscribe(TEST_TOPIC)
    time.sleep(20)
    consumer.disconnect()
    manager.stop()


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s :%(name)s:%(message)s',
        level=logging.INFO
    )
    # producer()
    threading._start_new_thread(consumer, ())
    threading._start_new_thread(consumer, ())
    time.sleep(5)
    threading._start_new_thread(producer, ())
    time.sleep(30)
