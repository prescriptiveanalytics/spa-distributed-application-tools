import asyncio
import json
import logging
import uuid
from contextlib import AbstractAsyncContextManager
from typing import Callable

import aiomqtt
import backoff
from pydantic.dataclasses import dataclass

from spa_dat.protocol.typedef import SocketProvider, SpaMessage, SpaSocket

logger = logging.getLogger(__name__)

MessageDecoder = Callable[[aiomqtt.Message], SpaMessage]
MessageEncoder = Callable[[SpaMessage], bytes]


# region helper functions
def _mqtt_message_decoder(message: aiomqtt.Message) -> SpaMessage:
    # decodes the mqtt message and builds the SpaMessage from it
    try:
        message = SpaMessage(**json.loads(message.payload.decode()))
        return message
    except json.JSONDecodeError as e:
        logger.error(f"Could not parse SPA message: {message.payload.decode()}. Error during `json.loads`: {e}")


def _mqtt_message_encoder(message: SpaMessage) -> bytes:
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
async def _read_response_message(client: aiomqtt.Client, message_decoder: MessageDecoder) -> SpaMessage | None:
    """
    Read a response message from the given topic. Returns None if no message was received / could not be parsed.
    Creates a new connection to avoid mixing messages with the default connection.
    """
    async with client.messages() as messages:
        async for message in messages:
            message = message_decoder(message)
            return message


# endregion helper functions


@dataclass
class MqttConfig:
    host: str
    port: int
    username = None
    password = None
    keepalive: int = 60
    qos: int = 0
    retain = False
    default_subscription_topics: list[str] | str | None = None  # if set to none no subscription will be made
    client_id: str | None = None


class MqttSocket(SpaSocket, AbstractAsyncContextManager):
    """
    Defines an interface for an mqtt broker. It implements all necessary methods from the SpaProtocol.
    """

    def __init__(
        self,
        config: MqttConfig,
        message_queue: asyncio.Queue | None,
        message_decoder: MessageDecoder = _mqtt_message_decoder,
        message_encoder: MessageEncoder = _mqtt_message_encoder,
    ) -> None:
        self.config = config
        # if no message queue is given create an internal one (for later access,
        # it contains all messages which are received from the broker)
        self.message_queue = message_queue if message_queue is not None else asyncio.Queue()
        self.message_decoder = message_decoder
        self.message_encoder = message_encoder
        self._client_config = self.build_client_config()
        self.client = aiomqtt.Client(**self._client_config)
        self.reader_task = None

    def build_client_config(self, client_id: str | None = None) -> dict:
        """
        Build a client config for a new client. The client_id is overwritten from the default if specified.
        """
        return dict(
            hostname=self.config.host,
            port=self.config.port,
            keepalive=self.config.keepalive,
            client_id=self.config.client_id if client_id is None else client_id,
            username=self.config.username,
            password=self.config.password,
        )

    async def __aenter__(self):
        """Return `self` upon entering the runtime context."""
        await self.client.connect()

        # spawn tasks which reads messages
        if self.reader_task is None:
            self.reader_task = asyncio.create_task(
                _read_messages(self.client, self.message_queue, self.message_decoder),
                name="task-mqtt-reader",
            )

        # subscribe to default topic(s)
        if self.config.default_subscription_topics is not None:
            if isinstance(self.config.default_subscription_topics, str):
                await self.subscribe(self.config.default_subscription_topics)
            if isinstance(self.config.default_subscription_topics, list):
                for topic in self.config.default_subscription_topics:
                    await self.subscribe(topic)

        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Raise any exception triggered within the runtime context."""
        if self.reader_task is not None:
            self.reader_task.cancel()
            self.reader_task = None

        await self.client.disconnect()
        return None

    async def publish(self, message: SpaMessage) -> None:
        await self.client.publish(message.topic, payload=self.message_encoder(message), qos=self.config.qos)

    async def subscribe(self, topic: str) -> None:
        await self.client.subscribe(topic, self.config.qos)
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
        config = self.build_client_config(client_id=f"{self.config.client_id}-response-{uuid.uuid4()}")
        async with aiomqtt.Client(**config) as client:
            await client.subscribe(ephemeral_response_topic)

            # start listener for response
            listener = _read_response_message(client, self.message_decoder)

            # publish message and wait for response, set response topic
            message.response_topic = ephemeral_response_topic

            await self.publish(message)

            # wait for response
            response = await listener
            await client.unsubscribe(ephemeral_response_topic)
        return response


class MqttSocketProvider(SocketProvider):
    """
    Defines an interface for a socket provider. A socket provider is a function which creates a socket and returns it.
    """

    def __init__(
        self,
        config: MqttConfig,
        message_decoder: MessageDecoder = _mqtt_message_decoder,
        message_encoder: MessageEncoder = _mqtt_message_encoder,
    ) -> None:
        self.config = config
        self.message_decoder = message_decoder
        self.message_encoder = message_encoder

    def overwrite_config(self, topics: str | list[str] | None = None, *kwargs) -> None:
        """
        Overwrites the given config from any defaults which were provided earlier. This is useful if you want to
        construct or change the default config
        """
        # normalize and set topics in config
        topics = topics or self.config.default_subscription_topics
        if topics is not None and isinstance(topics, str):
            topics = [topics]
        self.config.default_subscription_topics = topics

    def create_socket(
        self,
        queue: asyncio.Queue | None,
    ) -> None:
        return MqttSocket(self.config, queue, self.message_decoder, self.message_encoder)
