import logging

from spa_dat.application import ConsumerApplication, DistributedApplicationContext
from spa_dat.protocol.mqtt import MqttConfig
from spa_dat.protocol.typedef import SpaMessage
from spa_dat.provider import SocketProviderFactory

logger = logging.getLogger(__name__)


async def consumer_callback(message: SpaMessage, context: DistributedApplicationContext):
    logging.info(f"Received message: {message}")


def main():
    logging.basicConfig(level=logging.DEBUG)
    app = ConsumerApplication(
        consumer_callback,
        SocketProviderFactory.from_config(
            MqttConfig(
                host="mqtt-dashboard.com",
                port=1883,
                default_subscription_topics="test/spa-dat",
            )
        ),
    )
    app.start()


if __name__ == "__main__":
    main()
