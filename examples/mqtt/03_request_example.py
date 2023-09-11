import asyncio
import logging
import sys
import time

from spa_dat.application import (
    ConsumerApplication,
    DistributedApplicationContext,
    ProducerApplication,
)
from spa_dat.protocol.mqtt import MqttConfig
from spa_dat.protocol.typedef import SpaMessage
from spa_dat.provider import SocketProviderFactory

logger = logging.getLogger(__name__)


async def producer_callback(context: DistributedApplicationContext):
    for i in range(10):
        logger.info("Sending Request")
        response = await context.message_service.request(
            SpaMessage(
                client_id="spa-dat-producer",
                client_name="spa-dat-producer",
                content_type="application/json",
                payload=f"Producer Message {i}",
                topic="test/spa-dat-producer",
                timestamp=int(time.time()),
            )
        )
        logging.info(f"Received Response: {response.payload}")


async def consumer_callback(message: SpaMessage, context: DistributedApplicationContext):
    logger.info(f"Received Request: {message.payload}")

    # simulate long running request
    await asyncio.sleep(1)

    await context.message_service.publish(
        SpaMessage(
            client_name="spa-dat-responder",
            content_type="application/json",
            payload=f"Response Message for {message.payload}",
            topic=message.response_topic,
        )
    )


def run_consumer():
    consumer = ConsumerApplication(
        consumer_callback,
        SocketProviderFactory.from_config(
            MqttConfig(
                host="localhost",
                port=1883,
                default_subscription_topics="test/spa-dat-producer",
            )
        ),
    )
    consumer.run()


def run_producer():
    producer = ProducerApplication(
        producer_callback,
        SocketProviderFactory.from_config(
            MqttConfig(
                host="localhost",
                port=1883,
            )
        ),
    )
    producer.run()


def main():
    """
    Run this example twice in two different terminals. One as consumer and one as producer.
    """
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) != 2:
        logger.error("Please specify either 'consumer' or 'producer' as argument")
    if sys.argv[1] == "consumer":
        run_consumer()
    if sys.argv[1] == "producer":
        run_producer()
    else:
        logger.error("Please specify either 'consumer' or 'producer' as argument")


if __name__ == "__main__":
    main()
