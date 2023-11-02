import logging
import time

from spa_dat.application.application import DistributedApplication
from spa_dat.config import PayloadFormat, SocketConfig
from spa_dat.provider import SocketProviderFactory
from spa_dat.socket.mqtt import MqttConfig
from spa_dat.socket.typedef import SpaMessage, SpaSocket

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


socket_provider = SocketProviderFactory.from_config(
    SocketConfig(payload_format=PayloadFormat.JSON, socket_config=MqttConfig(host="mqtt-dashboard.com", port=1883))
)
app = DistributedApplication(socket_provider)


@app.producer()
async def producer(socket: SpaSocket, **kwargs):
    for i in range(10):
        await socket.publish(
            SpaMessage(
                Payload=f"Producer Message {i}",
                Topic="test/spa-dat",
            )
        )


app.start()
