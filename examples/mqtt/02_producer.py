import logging

from spa_dat.application.application import DistributedApplication
from spa_dat.config import PayloadFormat, SocketConfig
from spa_dat.provider import SocketProviderFactory
from spa_dat.socket.mqtt import MqttConfig
from spa_dat.socket.typedef import MessageBuilder, SpaMessage, SpaSocket

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


socket_provider = SocketProviderFactory.from_config(
    SocketConfig(payload_format=PayloadFormat.JSON, socket_config=MqttConfig(host="mqtt-dashboard.com", port=1883))
)
app = DistributedApplication(socket_provider)


@app.producer()
async def producer(socket: SpaSocket, message_builder: MessageBuilder, **kwargs):
    for i in range(10):
        await socket.publish(
            message_builder(
                payload=f"Producer Message {i}",
                topic="test/spa-dat",
            )
        )


app.start()
