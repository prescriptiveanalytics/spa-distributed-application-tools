import logging

from spa_dat.application.application import DistributedApplication
from spa_dat.config import PayloadFormat, SocketConfig
from spa_dat.provider import SocketProviderFactory
from spa_dat.socket.mqtt import MqttConfig
from spa_dat.socket.typedef import SpaMessage

logging.basicConfig(level=logging.DEBUG)


socket_provider = SocketProviderFactory.from_config(
    SocketConfig(payload_format=PayloadFormat.JSON, socket_config=MqttConfig(host="mqtt-dashboard.com", port=1883))
)
app = DistributedApplication(socket_provider)


@app.application("test/spa-dat")
async def consumer(
    message: SpaMessage,
    # socket: SpaSocket, <- not needed but available if communication is required
    **kwargs,
):
    logging.info(f"Received message: {message}")


app.start()
