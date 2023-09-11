import logging
import time

from spa_dat.application import DistributedApplication, DistributedApplicationContext
from spa_dat.protocol.mqtt import MqttConfig
from spa_dat.protocol.typedef import SpaMessage
from spa_dat.provider import SocketProviderFactory

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


socket_provider = SocketProviderFactory.from_config(MqttConfig(host="mqtt-dashboard.com", port=1883))
app = DistributedApplication(socket_provider)


@app.producer()
async def producer_callback(context: DistributedApplicationContext):
    for i in range(10):
        await context.message_service.publish(
            SpaMessage(
                payload=f"Producer Message {i}",
                topic="test/spa-dat",
                timestamp=int(time.time()),
            )
        )


app.start()
