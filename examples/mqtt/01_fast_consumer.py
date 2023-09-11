import logging

from spa_dat.application import (
    DistributedApplicationContext,
    FastDistributedApplication,
)
from spa_dat.protocol.mqtt import MqttConfig
from spa_dat.protocol.typedef import SpaMessage
from spa_dat.provider import SocketProviderFactory

logging.basicConfig(level=logging.DEBUG)


socket_provider = SocketProviderFactory.from_config(
    MqttConfig(host="mqtt-dashboard.com", port=1883, default_subscription_topics="test/spa-dat")
)
app = FastDistributedApplication(socket_provider)


@app.application()
async def consumer_callback(message: SpaMessage, context: DistributedApplicationContext):
    logging.info(f"Received message: {message}")


app.run()
