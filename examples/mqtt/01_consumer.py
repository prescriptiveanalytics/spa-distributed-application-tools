import logging

from spa_dat.application.application import DistributedApplication
from spa_dat.protocol.mqtt import MqttConfig
from spa_dat.protocol.typedef import SpaMessage
from spa_dat.provider import SocketProviderFactory

logging.basicConfig(level=logging.DEBUG)


socket_provider = SocketProviderFactory.from_config(MqttConfig(host="mqtt-dashboard.com", port=1883))
app = DistributedApplication(socket_provider)


@app.application("test/spa-dat")
async def consumer(
    message: SpaMessage,
    # socket: SpaSocket, <- not needed but available if communication is required
    **kwargs
):
    logging.info(f"Received message: {message}")


app.start()
