import asyncio
import json
import logging
from typing import Callable
import uuid
from contextlib import AbstractAsyncContextManager

import aiomqtt
import backoff

from spa_dat.config import MqttConfig
from spa_dat.protocol.spa import SpaMessage, SpaProtocol

logger = logging.getLogger(__name__)

MessageDecoder = Callable[[aiomqtt.Message], SpaMessage]
MessageEncoder = Callable[[SpaMessage], aiomqtt.Message]


def _mqtt_message_decoder(message: aiomqtt.Message) -> SpaMessage:
    # decodes the mqtt message and builds the SpaMessage from it
    try:
        message = SpaMessage(**json.loads(message.payload.decode()))
        return message
    except json.JSONDecodeError as e:
        logger.error(f"Could not parse SPA message: {message.payload.decode()}. Error during `json.loads`: {e}")


def _mqtt_message_encoder(message: SpaMessage) -> aiomqtt.Message:
    return message.model_dump_json().encode("utf-8")


@backoff.on_exception(backoff.expo, aiomqtt.MqttError, jitter=backoff.random_jitter, logger=logger)
async def _read_messages(client: aiomqtt.Client, message_queue: asyncio.Queue, message_decoder: MessageDecoder):
    async with client.messages() as messages:
        async for message in messages:
            message = message_decoder(message)
            if message is not None:
                await message_queue.put(message)


@backoff.on_exception(backoff.expo, aiomqtt.MqttError, jitter=backoff.random_jitter, logger=logger)
async def _read_single_message(client: aiomqtt.Client, message_decoder: MessageDecoder) -> SpaMessage:
    async with client.messages() as messages:
        async for message in messages:
            return message_decoder(message)


class MqttService(SpaProtocol, AbstractAsyncContextManager):
    def __init__(
        self,
        config: MqttConfig,
        message_queue: asyncio.Queue = asyncio.Queue(),
        message_decoder: MessageDecoder = _mqtt_message_decoder,
        message_encoder: MessageEncoder = _mqtt_message_encoder,
    ) -> None:
        self.mqtt_config = config
        self.message_queue = message_queue
        self.message_decoder = message_decoder
        self.message_encoder = message_encoder
        self.client = aiomqtt.Client(
            hostname=config.host,
            port=config.port,
            keepalive=config.keepalive,
            client_id=config.client_id,
            username=config.username,
            password=config.password,
        )
        self.reader_task = None
        self.subscriptions: dict[str, asyncio.Task] = {}

    async def __aenter__(self):
        """Return `self` upon entering the runtime context."""
        await self.client.connect()

        # spawn tasks which reads messages
        self.reader_task = asyncio.create_task(
            _read_messages(self.client, self.message_queue, self.message_decoder),
            name="task-mqtt-reader",
        )

        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Raise any exception triggered within the runtime context."""
        if self.reader_task is not None:
            self.reader_task.cancel()
            self.reader_task = None

        await self.client.disconnect()
        return None

    async def publish(self, message: SpaMessage) -> None:
        await self.client.publish(message.topic, payload=self.message_encoder(message), qos=message.quality_of_service)

    async def subscribe(self, topic: str) -> None:
        # subscribe to topic
        await self.client.subscribe(topic, self.mqtt_config.qos)
        logger.info(f"Subscribed to topic: {topic}")

    async def unsubscribe(self, topic: str) -> None:
        await self.client.unsubscribe(topic)
        logger.info(f"Unsubscribed from topic: {topic}")

    def _get_ephemeral_response_topic(self, topic: str) -> str:
        return f"{topic}/request/{uuid.uuid4()}"

    async def request(self, topic: str, payload: bytes) -> SpaMessage:
        ephemeral_response_topic = self._get_ephemeral_response_topic(topic)
        await self.subscribe(ephemeral_response_topic)

        # publish message and wait for response
        await self.publish(topic, payload)
        response = await _read_single_message(
            self.client, self.mqtt_config, ephemeral_response_topic, self.message_queue, self.message_decoder
        )

        await self.unsubscribe(ephemeral_response_topic)
        return response
