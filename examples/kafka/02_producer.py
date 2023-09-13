import logging

from spa_dat.application.application import DistributedApplication
from spa_dat.config import PayloadFormat, SocketConfig
from spa_dat.provider import SocketProviderFactory
from spa_dat.socket.kafka import KafkaConfig
from spa_dat.socket.typedef import SpaMessage, SpaSocket

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


socket_provider = SocketProviderFactory.from_config(
    SocketConfig(payload_format=PayloadFormat.JSON, socket_config=KafkaConfig(bootstrap_servers="localhost:9092"))
)
app = DistributedApplication(socket_provider)


@app.producer()
async def producer(socket: SpaSocket, **kwargs):
    for i in range(10):
        await socket.publish(
            SpaMessage(
                payload=f"Producer Message {i}",
                topic="test-spa-dat-producer",
            )
        )


app.start()
