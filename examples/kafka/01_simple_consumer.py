import logging

from spa_dat.application import ConsumerApplication, DistributedApplicationContext
from spa_dat.protocol.kafka import KafkaConfig
from spa_dat.protocol.typedef import SpaMessage
from spa_dat.provider import SocketProviderFactory

logger = logging.getLogger(__name__)


async def consumer_callback(message: SpaMessage, context: DistributedApplicationContext):
    logging.info(f"Received message: {message}")


def main():
    logging.basicConfig(level=logging.INFO)
    app = ConsumerApplication(
        consumer_callback,
        SocketProviderFactory.from_config(
            KafkaConfig(
                bootstrap_servers="localhost:9092",
                default_subscription_topics="spa-dat2",
                group_id="spa-dat-test",
            )
        ),
    )
    app.run()


if __name__ == "__main__":
    main()
