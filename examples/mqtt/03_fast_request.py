import asyncio
import logging
import time

from spa_dat.application import (
    DistributedApplicationContext,
    DistributedApplication,
)
from spa_dat.protocol.mqtt import MqttConfig
from spa_dat.protocol.typedef import SpaMessage
from spa_dat.provider import SocketProviderFactory

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


socket_provider = SocketProviderFactory.from_config(MqttConfig(host="mqtt-dashboard.com", port=1883))
app = DistributedApplication(default_socket_provider=socket_provider)


@app.producer()
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
                quality_of_service=1,
                timestamp=int(time.time()),
            )
        )
        logging.info(f"Received Response: {response.payload}")


@app.application('test/spa-dat-producer')
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


app.run()