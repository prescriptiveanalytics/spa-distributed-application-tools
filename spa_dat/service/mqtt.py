import asyncio
import logging

import aiomqtt
import backoff

from spa_dat.config import MqttConfig
from spa_dat.protocol.spa import SpaProtocol

logger = logging.getLogger(__name__)


def _create_aiomqtt_client(config: MqttConfig):
    return aiomqtt.Client(
        hostname=config.host,
        port=config.port,
        keepalive=config.keepalive,
        client_id=config.client_id,
        username=config.username,
        password=config.password,
        # config.qos,
        # config.retain,
    )


@backoff.on_exception(backoff.expo, aiomqtt.MqttError, jitter=backoff.random_jitter, logger=logger)
async def _subscribe(config: MqttConfig, topic: str, message_queue: asyncio.Queue):
    async with _create_aiomqtt_client(config) as client:
        await client.subscribe(topic, qos=config.qos)
        logger.info(f"Subscribed to topic: {topic}")
        async with client.messages() as messages:
            async for message in messages:
                await message_queue.put(message)


class MqttService(SpaProtocol):
    def __init__(self, config: MqttConfig, message_queue: asyncio.Queue) -> None:
        self.config = config
        self.message_queue = message_queue
        self.client = _create_aiomqtt_client(config)
        self.subscriptions: dict[str, asyncio.Task] = {}

    async def publish(self, topic: str, payload: bytes):
        await self.client.publish(topic, payload=payload)

    def subscribe(self, topic: str):
        task_name = f"task-mqtt-subscripition-{topic}"
        self.subscriptions[topic] = asyncio.create_task(_subscribe(self.config, topic, self.message_queue), name=task_name)

    async def unsubscribe(self, topic: str):
        if topic in self.subscriptions:
            task = self.subscriptions[topic]
            task.cancel()
            del self.subscriptions[topic]
            logger.info(f"Unsubscribed from topic: {topic}, task: {task.get_name()}")
        else:
            logger.warning(f"Cannot unsubscribe from topic: {topic}, no subscription found")

    async def request():
        raise NotImplementedError()
