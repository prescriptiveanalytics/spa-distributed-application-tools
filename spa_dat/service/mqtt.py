import asyncio
import json
import logging
import uuid
from contextlib import AbstractAsyncContextManager
from typing import Callable

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
    """
    Encodes the SpaMessage into a mqtt message.
    """
    return message.model_dump_json().encode("utf-8")


@backoff.on_exception(backoff.expo, aiomqtt.MqttError, jitter=backoff.random_jitter, logger=logger)
async def _read_messages(client: aiomqtt.Client, message_queue: asyncio.Queue, message_decoder: MessageDecoder):
    async with client.messages() as messages:
        async for message in messages:
            message = message_decoder(message)
            if message is not None:
                await message_queue.put(message)


@backoff.on_exception(backoff.expo, aiomqtt.MqttError, jitter=backoff.random_jitter, logger=logger)
async def _read_response_message(
    client: aiomqtt.Client, ephemeral_response_topic: str, message_decoder: MessageDecoder
) -> SpaMessage | None:
    """
    Read a response message from the given topic. Returns None if no message was received / could not be parsed.
    Creates a new connection to avoid mixing messages with the default connection.
    """
    async with client.messages() as messages:
        async for message in messages:
            message = message_decoder(message)
            return message


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
        self._client_config = self.build_client_config()        
        self.client = aiomqtt.Client(**self._client_config)
        self.reader_task = None
        self.subscriptions: dict[str, asyncio.Task] = {}
        
    def build_client_config(self, client_id: str | None = None) -> dict:
        """
        Build a client config for a new client. The client_id is overwritten from the default if specified.
        """
        return dict(
            hostname=self.mqtt_config.host,
            port=self.mqtt_config.port,
            keepalive=self.mqtt_config.keepalive,
            client_id=self.mqtt_config.client_id if client_id is None else client_id,
            username=self.mqtt_config.username,
            password=self.mqtt_config.password,
        )

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
        await self.client.subscribe(topic, self.mqtt_config.qos)
        logger.info(f"Subscribed to topic: {topic}")

    async def unsubscribe(self, topic: str) -> None:
        await self.client.unsubscribe(topic)
        logger.info(f"Unsubscribed from topic: {topic}")

    def _get_ephemeral_response_topic(self, topic: str) -> str:
        return f"{topic}/request/{uuid.uuid4()}"

    async def request(self, message: SpaMessage) -> SpaMessage | None:
        """
        Publish a message and wait for a response. Returns none if no response was received / could not be parsed.
        """
        ephemeral_response_topic = self._get_ephemeral_response_topic(message.topic)

        # we must build a new client .. otherwise the background listener will receive the response
        config = self.build_client_config(client_id=f"{self.mqtt_config.client_id}-response-{uuid.uuid4()}")        
        async with aiomqtt.Client(**config) as client:
            await client.subscribe(ephemeral_response_topic)

            # start listener for response
            listener = _read_response_message(client, ephemeral_response_topic, self.message_decoder)

            # publish message and wait for response, set response topic
            message.response_topic = ephemeral_response_topic
            await self.publish(message)

            # wait for response
            response = await listener
            await client.unsubscribe(ephemeral_response_topic)
        return response
