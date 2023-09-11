import logging

from spa_dat.application.application import DistributedApplication
from spa_dat.protocol.kafka import KafkaConfig
from spa_dat.protocol.typedef import SpaMessage
from spa_dat.provider import SocketProviderFactory

logging.basicConfig(level=logging.INFO)


socket_provider = SocketProviderFactory.from_config(KafkaConfig(bootstrap_servers="localhost:9092"))
app = DistributedApplication(socket_provider)


@app.application("spa-dat2")
async def consumer(
    message: SpaMessage,
    # socket: SpaSocket <- not needed but available if communication is required
):
    logging.info(f"Received message: {message}")


app.start()
