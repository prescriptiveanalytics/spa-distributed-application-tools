import logging

from spa_dat.application.application import DistributedApplication
from spa_dat.protocol.kafka import KafkaConfig
from spa_dat.protocol.typedef import SpaMessage, SpaSocket
from spa_dat.provider import SocketProviderFactory

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


socket_provider = SocketProviderFactory.from_config(KafkaConfig(bootstrap_servers="localhost:9092"))
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
