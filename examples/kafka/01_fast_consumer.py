import logging

from spa_dat.application import (
    DistributedApplicationContext,
    DistributedApplication,
)
from spa_dat.protocol.kafka import KafkaConfig
from spa_dat.protocol.mqtt import MqttConfig
from spa_dat.protocol.typedef import SpaMessage
from spa_dat.provider import SocketProviderFactory

logging.basicConfig(level=logging.INFO)


socket_provider = SocketProviderFactory.from_config(
    KafkaConfig(bootstrap_servers="localhost:9092")
)
app = DistributedApplication(socket_provider)


@app.application('spa-dat2')
async def consumer_callback(message: SpaMessage, context: DistributedApplicationContext):
    logging.info(f"Received message: {message}")


app.run()
