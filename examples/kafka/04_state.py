import asyncio
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
app = DistributedApplication(default_socket_provider=socket_provider)


@app.producer()
async def producer(socket: SpaSocket, **kwargs):
    for i in range(10):
        _ = await socket.request(
            SpaMessage(
                Payload=f"Producer Message {i}",
                Topic="test-spa-dat-producer",
            )
        )


class ConsumerState:
    counter: int = 0


@app.application("test-spa-dat-producer", state=ConsumerState())
async def consumer(
    message: SpaMessage,
    socket: SpaSocket,
    state: ConsumerState,
    **kwargs,
):
    state.counter += 1
    logger.info(f"Received Request: {state.counter}")

    # simulate long running request
    await asyncio.sleep(1)

    await socket.publish(
        SpaMessage(
            Payload=f"Response Message for {message.payload}",
            Topic=message.response_topic,
        )
    )


app.start()
