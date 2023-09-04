import logging
import time

from spa_dat.application import FastDistributedApplication, DistributedApplicationContext
from spa_dat.protocol.mqtt import MqttConfig
from spa_dat.protocol.typedef import SpaMessage
from spa_dat.provider import SocketProviderFactory

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


socket_provider = SocketProviderFactory.from_config(MqttConfig(host="mqtt-dashboard.com", port=1883))
app = FastDistributedApplication(socket_provider)


@app.producer()
async def producer_callback(context: DistributedApplicationContext):
    for i in range(10):
        await context.message_service.publish(
            SpaMessage(
                client_id="spa-dat-producer",
                client_name="spa-dat-producer",
                content_type="application/json",
                payload="Producer Message",
                topic="test/spa-dat",
                response_topic="test/spa-dat-producer-response",
                quality_of_service=1,
                timestamp=int(time.time()),
            )
        )


app.run()