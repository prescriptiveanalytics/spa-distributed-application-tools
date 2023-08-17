import asyncio
from contextlib import AbstractAsyncContextManager, asynccontextmanager
import logging
import uuid

import aiomqtt
import backoff

from spa_dat.config import MqttConfig
from spa_dat.protocol.spa import SpaMessage, SpaProtocol

logger = logging.getLogger(__name__)


@backoff.on_exception(backoff.expo, aiomqtt.MqttError, jitter=backoff.random_jitter, logger=logger)
async def _read_messages(client: aiomqtt.Client, config: MqttConfig, topic: str, message_queue: asyncio.Queue):
    async with client.messages() as messages:
        async for message in messages:
            await message_queue.put(message)
            
@backoff.on_exception(backoff.expo, aiomqtt.MqttError, jitter=backoff.random_jitter, logger=logger)
async def _read_single_message(client: aiomqtt.Client, config: MqttConfig, topic: str, message_queue: asyncio.Queue):
    async with client.messages() as messages:
        async for message in messages:
            return message


class MqttService(SpaProtocol, AbstractAsyncContextManager):
    def __init__(self, config: MqttConfig, message_queue: asyncio.Queue = None) -> None:
        self.mqtt_config = config
        self.message_queue = message_queue if message_queue is not None else asyncio.Queue()
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

    async def publish(self, message: SpaMessage):
        await self.client.publish(message.topic, payload=message.model_dump_json().encode('utf-8'))

    async def subscribe(self, topic: str):
        # subscribe to topic
        task_name = MqttService._get_task_name(topic)
        await self.client.subscribe(topic, self.mqtt_config.qos)
        logger.info(f"Subscribed to topic: {topic}")

        # spawn tasks which reads messages
        self.subscriptions[task_name] = asyncio.create_task(
            _read_messages(self.client, self.mqtt_config, topic, self.message_queue), 
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

    def _get_ephemeral_response_topic(self, topic: str):
        return f"{topic}/request/{uuid.uuid4()}"

    async def request(self, topic: str, payload: bytes):
        ephemeral_response_topic = self._get_ephemeral_response_topic(topic)
        await self.subscribe(ephemeral_response_topic)
        
        # publish message and wait for response
        await self.publish(topic, payload)
        response = await _read_single_message(self.client, self.mqtt_config, ephemeral_response_topic, self.message_queue)
        
        await self.unsubscribe(ephemeral_response_topic)
        return response