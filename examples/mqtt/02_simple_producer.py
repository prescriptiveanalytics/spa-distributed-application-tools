import asyncio
import logging
import time

import aiomqtt

from spa_dat.application import DistributedApplicationContext, ProducerApplication
from spa_dat.config import MqttConfig
from spa_dat.protocol.spa import SpaMessage


logger = logging.getLogger(__name__)

async def producer_callback(context: DistributedApplicationContext):
    for i in range(10):
        await context.message_service.publish(SpaMessage(
            client_id="spa-dat-producer",
            client_name="spa-dat-producer",
            content_type="application/json",
            payload="Producer Message",
            topic="test/spa-dat-producer",
            response_topic="test/spa-dat-producer-response",
            quality_of_service=1,
            timestamp=int(time.time())
        ))


def main():
    logging.basicConfig(level=logging.DEBUG)
    app = ProducerApplication(producer_callback, MqttConfig(
        host="mqtt-dashboard.com",
        port=1883,
    ))
    app.run()


if __name__ == "__main__":
    main()
