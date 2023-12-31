import logging

from spa_dat.application.application import DistributedApplication
from spa_dat.config import PayloadFormat, SocketConfig
from spa_dat.provider import SocketProviderFactory
from spa_dat.socket.kafka import KafkaConfig
from spa_dat.socket.typedef import SpaMessage

logging.basicConfig(level=logging.INFO)


socket_provider = SocketProviderFactory.from_config(
    SocketConfig(payload_format=PayloadFormat.JSON, socket_config=KafkaConfig(bootstrap_servers="localhost:9092"))
)
app = DistributedApplication(socket_provider)


@app.application("spa-dat2")
async def consumer(
    message: SpaMessage,
    # socket: SpaSocket, <- not needed but available if communication is required
    **kwargs,
):
    logging.info(f"Received message: {message}")


app.start()
