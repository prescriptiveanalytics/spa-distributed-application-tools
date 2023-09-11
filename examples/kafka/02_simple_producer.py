import logging
import time

from spa_dat.application import AbstractApplication, DistributedApplicationContext
from spa_dat.protocol.kafka import KafkaConfig
from spa_dat.protocol.typedef import SpaMessage
from spa_dat.provider import SocketProviderFactory

logger = logging.getLogger(__name__)


async def producer_callback(context: DistributedApplicationContext):
    for i in range(10):
        await context.message_service.publish(
            SpaMessage(
                client_id="spa-dat-producer",
                client_name="spa-dat-producer",
                content_type="application/json",
                payload="Producer Message",
                topic="spa-dat",
                response_topic="test/spa-dat-producer-response",
                quality_of_service=1,
                timestamp=int(time.time()),
            )
        )


def main():
    logging.basicConfig(level=logging.DEBUG)
    app = AbstractApplication(
        producer_callback,
        SocketProviderFactory.from_config(
            KafkaConfig(
                bootstrap_servers="localhost:9092",
                group_id="spa-dat-test",
            )
        ),
    )
    app.start()


if __name__ == "__main__":
    main()
