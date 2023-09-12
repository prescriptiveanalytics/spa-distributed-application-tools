import asyncio
import logging

from traitlets import Any

from spa_dat.application.application import DistributedApplication
from spa_dat.protocol.kafka import KafkaConfig
from spa_dat.protocol.typedef import SpaMessage, SpaSocket
from spa_dat.provider import SocketProviderFactory

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


socket_provider = SocketProviderFactory.from_config(KafkaConfig(bootstrap_servers="localhost:9092"))
app = DistributedApplication(default_socket_provider=socket_provider)


@app.producer()
async def producer(
    socket: SpaSocket, 
    **kwargs
):
    for i in range(10):
        _ = await socket.request(
            SpaMessage(
                payload=f"Producer Message {i}",
                topic="test-spa-dat-producer",
            )
        )


class ConsumerState:
    counter: int = 0


@app.application("test-spa-dat-producer", state=ConsumerState())
async def consumer(
    message: SpaMessage, 
    socket: SpaSocket, 
    state: ConsumerState, 
):
    state.counter += 1
    logger.info(f"Received Request: {state.counter}")

    # simulate long running request
    await asyncio.sleep(1)

    await socket.publish(
        SpaMessage(
            payload=f"Response Message for {message.payload}",
            topic=message.response_topic,
        )
    )


app.start()
