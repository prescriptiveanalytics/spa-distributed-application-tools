import asyncio
import logging

from spa_dat.application.application import DistributedApplication
from spa_dat.config import PayloadFormat, SocketConfig
from spa_dat.provider import SocketProviderFactory
from spa_dat.socket.mqtt import MqttConfig
from spa_dat.socket.typedef import MessageBuilder, SpaMessage, SpaSocket

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


socket_provider = SocketProviderFactory.from_config(
    SocketConfig(payload_format=PayloadFormat.JSON, socket_config=MqttConfig(host="mqtt-dashboard.com", port=1883))
)
app = DistributedApplication(default_socket_provider=socket_provider)


@app.producer()
async def producer(socket: SpaSocket, message_builder: MessageBuilder, **kwargs):
    for i in range(10):
        logger.info("Sending Request")
        response = await socket.request(
            message_builder(
                payload=f"Producer Message {i}",
                topic="test/spa-dat-producer",
            )
        )
        logging.info(f"Received Response: {response.payload}")


@app.application("test/spa-dat-producer")
async def consumer(message: SpaMessage, message_builder: MessageBuilder, socket: SpaSocket, **kwargs):
    logger.info(f"Received Request: {message.payload}")

    # simulate long running request
    await asyncio.sleep(1)

    await socket.publish(
        message_builder(
            payload=f"Response Message for {message.payload}",
            topic=message.response_topic,
        )
    )


app.start()
