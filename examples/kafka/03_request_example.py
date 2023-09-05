import asyncio
import logging
import sys
import time

from spa_dat.application import (
    DistributedApplication,
    DistributedApplicationContext,
    ProducerApplication,
)
from spa_dat.protocol.kafka import KafkaConfig
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
                topic="spa-dat-producer",
                quality_of_service=1,
                timestamp=int(time.time()),
            )
        )
        logging.debug(f"Received Response: {response.payload}")


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
    consumer = DistributedApplication(
        consumer_callback,
        SocketProviderFactory.from_config(
            KafkaConfig(
                bootstrap_servers="localhost:9092",
                default_subscription_topics="spa-dat-producer",
            )
        ),
    )
    consumer.run()


def run_producer():
    producer = ProducerApplication(
        producer_callback,
        SocketProviderFactory.from_config(
            KafkaConfig(
                bootstrap_servers="localhost:9092",
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
