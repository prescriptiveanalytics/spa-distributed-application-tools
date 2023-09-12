import logging
import time

from spa_dat.application.application import DistributedApplication
from spa_dat.protocol.mqtt import MqttConfig
from spa_dat.protocol.typedef import SpaMessage, SpaSocket
from spa_dat.provider import SocketProviderFactory

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


socket_provider = SocketProviderFactory.from_config(MqttConfig(host="mqtt-dashboard.com", port=1883))
app = DistributedApplication(socket_provider)


@app.producer()
async def producer(socket: SpaSocket, **kwargs):
    for i in range(10):
        await socket.publish(
            SpaMessage(
                payload=f"Producer Message {i}",
                topic="test/spa-dat",
                timestamp=int(time.time()),
            )
        )


app.start()