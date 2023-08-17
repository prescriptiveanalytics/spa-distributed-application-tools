import asyncio
import logging
import time

import aiomqtt

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
        response = await context.message_service.request(
            SpaMessage(
                client_id="spa-dat-producer",
                client_name="spa-dat-producer",
                content_type="application/json",
                payload="Producer Message",
                topic="test/spa-dat-producer",
                response_topic="test/spa-dat-producer-response",
                quality_of_service=1,
                timestamp=int(time.time()),
            )
        )
        logging.debug(f"Received Response: {response.payload.decode()}")


async def consumer_callback(message: aiomqtt.client.Message, context: DistributedApplicationContext):
    logging.debug(f"Received Request: {message.payload.decode()}")
    await context.message_service.publish(
        SpaMessage(
            client_id="spa-dat-responder",
            client_name="spa-dat-responder",
            content_type="application/json",
            payload="Resposne Message",
            topic=message.response_topic,
        )
    )


def main():
    logging.basicConfig(level=logging.DEBUG)
    producer = ProducerApplication(
        producer_callback,
        MqttConfig(
            host="mqtt-dashboard.com",
            port=1883,
        ),
    )
    consumer = DistributedApplication(
        consumer_callback,
        MqttConfig(
            host="mqtt-dashboard.com",
            port=1883,
            default_subscription_topic="test/spa-dat-producer",
        ),
    )

    with asyncio.Runner() as runner:
        runner.run(producer.run_async())
        runner.run(consumer.run_async())


if __name__ == "__main__":
    main()
