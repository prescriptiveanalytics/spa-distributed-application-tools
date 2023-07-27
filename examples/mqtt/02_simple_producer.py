import asyncio
import logging

import aiomqtt

from spa_dat.application import DistributedApplicationContext, DistributedApplication
from spa_dat.config import MqttConfig


logger = logging.getLogger(__name__)

async def producer_callback(message: aiomqtt.client.Message, context: DistributedApplicationContext):
    await context.message_service.publish("test/spa-dat", message.payload)
    logging.debug(f"Received message: {message.payload.decode()}")


def main():
    logging.basicConfig(level=logging.DEBUG)
    app = DistributedApplication(producer_callback, MqttConfig(
        host="mqtt-dashboard.com",
        port=1883,
        topic="test/spa-dat",
    ))
    app.run()


if __name__ == "__main__":
    main()
