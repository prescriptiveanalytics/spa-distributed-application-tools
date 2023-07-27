import asyncio
from contextlib import AbstractAsyncContextManager, asynccontextmanager
import logging

import aiomqtt
import backoff

from spa_dat.config import MqttConfig
from spa_dat.protocol.spa import SpaProtocol

logger = logging.getLogger(__name__)


@backoff.on_exception(backoff.expo, aiomqtt.MqttError, jitter=backoff.random_jitter, logger=logger)
async def _subscribe(client: aiomqtt.Client, config: MqttConfig, topic: str, message_queue: asyncio.Queue):
    async with client.messages() as messages:
        async for message in messages:
            await message_queue.put(message)


class MqttService(SpaProtocol, AbstractAsyncContextManager):
    def __init__(self, config: MqttConfig, message_queue: asyncio.Queue) -> None:
        self.mqtt_config = config
        self.message_queue = message_queue
        self.client = aiomqtt.Client(
            hostname=config.host,
            port=config.port,
            keepalive=config.keepalive,
            client_id=config.client_id,
            username=config.username,
            password=config.password,
        )
        self.subscriptions: dict[str, asyncio.Task] = {}
        
    @staticmethod
    def _get_task_name(topic: str):
        return f"task-mqtt-subscripition-{topic}"

    async def __aenter__(self):
        """Return `self` upon entering the runtime context."""
        await self.client.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Raise any exception triggered within the runtime context."""
        await self.client.disconnect()
        return None

    async def publish(self, topic: str, payload: bytes):
        await self.client.publish(topic, payload=payload)

    async def subscribe(self, topic: str):
        # subscribe to topic
        task_name = MqttService._get_task_name(topic)
        await self.client.subscribe(topic, self.mqtt_config.qos)
        logger.info(f"Subscribed to topic: {topic}")

        # spawn tasks which reads messages
        self.subscriptions[task_name] = asyncio.create_task(
            _subscribe(self.client, self.mqtt_config, topic, self.message_queue), 
            name=MqttService._get_task_name(task_name),
        )

    async def unsubscribe(self, topic: str):
        task_name = MqttService._get_task_name(topic)
        if task_name in self.subscriptions:
            await self.client.unsubscribe(topic)
            self.subscriptions[task_name].cancel()
            del self.subscriptions[task_name]
            logger.info(f"Unsubscribed from topic: {topic}")
        else:
            logger.info("No subscription to unsubscribe from. (searched for topic: {topic})")

    async def request():
        raise NotImplementedError()
