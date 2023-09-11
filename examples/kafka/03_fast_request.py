import asyncio
import logging
import time

from spa_dat.application import (
    DistributedApplicationContext,
    DistributedApplication,
)
from spa_dat.protocol.kafka import KafkaConfig
from spa_dat.protocol.typedef import SpaMessage
from spa_dat.provider import SocketProviderFactory

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


socket_provider = SocketProviderFactory.from_config(KafkaConfig(bootstrap_servers="localhost:9092"))
app = DistributedApplication(default_socket_provider=socket_provider)


@app.application('test-spa-dat-producer')
async def consumer_callback(message: SpaMessage, context: DistributedApplicationContext):
    logger.info(f"Received Request: {message.payload}")

    # simulate long running request
    await asyncio.sleep(1)

    await context.message_service.publish(
        SpaMessage(
            payload=f"Response Message for {message.payload}",
            topic=message.response_topic,
        )
    )



@app.producer()
async def producer_callback(context: DistributedApplicationContext):
    for i in range(10):
        logger.info("Sending Request")
        response = await context.message_service.request(
            SpaMessage(
                payload=f"Producer Message {i}",
                topic="test-spa-dat-producer",
            )
        )
        logging.info(f"Received Response: {response.payload}")



app.run()