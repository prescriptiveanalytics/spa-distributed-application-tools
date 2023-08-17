import asyncio
import logging
import sys
import time

from spa_dat.application import (
    DistributedApplication,
    DistributedApplicationContext,
    ProducerApplication,
)
from spa_dat.config import MqttConfig
from spa_dat.protocol.spa import SpaMessage

logger = logging.getLogger(__name__)


async def producer_callback(context: DistributedApplicationContext):
    for i in range(10):
        logger.debug("Sending Request")
        response = await context.message_service.request(
            SpaMessage(
                client_id=f"spa-dat-producer",
                client_name="spa-dat-producer",
                content_type="application/json",
                payload=f"Producer Message {i}",
                topic="test/spa-dat-producer",
                quality_of_service=1,
                timestamp=int(time.time()),
            )
        )
        logging.debug(f"Received Response: {response.payload}")


async def consumer_callback(message: SpaMessage, context: DistributedApplicationContext):
    logger.debug(f"Received Request: {message.payload}")

    # simulate long running request
    await asyncio.sleep(1)

    await context.message_service.publish(
        SpaMessage(
            client_id=context.message_service.mqtt_config.client_id,
            client_name="spa-dat-responder",
            content_type="application/json",
            payload=f"Response Message for {message.payload}",
            topic=message.response_topic,
        )
    )
    
def run_consumer():
    consumer = DistributedApplication(
        consumer_callback,
        MqttConfig(
            host="mqtt-dashboard.com",
            port=1883,
            default_subscription_topic="test/spa-dat-producer",
        ),
    )
    consumer.run()

def run_producer():
    producer = ProducerApplication(
        producer_callback,
        MqttConfig(
            host="mqtt-dashboard.com",
            port=1883,
        ),
    )
    producer.run()

def main():
    logging.basicConfig(level=logging.DEBUG)
    if sys.argv[1] == 'consumer':
        run_consumer()
    if sys.argv[1] == 'producer':
        run_producer()
    else:
        logger.error("Please specify either 'consumer' or 'producer' as argument")


if __name__ == "__main__":
    main()
