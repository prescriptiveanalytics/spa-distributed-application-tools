import logging
import time

from spa_dat.application import DistributedApplication, DistributedApplicationContext
from spa_dat.protocol.kafka import KafkaConfig
from spa_dat.protocol.typedef import SpaMessage
from spa_dat.provider import SocketProviderFactory

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


socket_provider = SocketProviderFactory.from_config(KafkaConfig(bootstrap_servers="localhost:9092"))
app = DistributedApplication(socket_provider)


@app.producer()
async def producer_callback(context: DistributedApplicationContext):
    for i in range(10):
        await context.message_service.publish(
            SpaMessage(
                payload=f"Producer Message {i}",
                topic="test-spa-dat-producer",
                timestamp=int(time.time()),
            )
        )


app.run()
