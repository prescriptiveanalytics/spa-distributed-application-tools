import asyncio
import copy
import logging
import uuid
from contextlib import AbstractAsyncContextManager
from typing import Callable, Self

import aiomqtt
import backoff
from pydantic.dataclasses import dataclass

from spa_dat.socket.typedef import SocketProvider, SpaMessage, SpaSocket
from spa_dat.serialization import Serializer

logger = logging.getLogger(__name__)

MessageDecoder = Callable[[aiomqtt.Message], bytes]


# region helper functions
def _mqtt_message_decoder(message: aiomqtt.Message) -> SpaMessage:
    """
    Decode the message value.
    """
    return message.payload.decode()


@backoff.on_exception(backoff.expo, aiomqtt.MqttError, jitter=backoff.random_jitter, logger=logger)
async def _read_messages(
    client: aiomqtt.Client, message_queue: asyncio.Queue, message_decoder: MessageDecoder, serializer: Serializer
):
    async with client.messages() as messages:
        async for message in messages:
            message = serializer.deserialize(message_decoder(message))
            if message is not None:
                await message_queue.put(message)


@backoff.on_exception(backoff.expo, aiomqtt.MqttError, jitter=backoff.random_jitter, logger=logger)
async def _read_response_message(client: aiomqtt.Client, message_decoder: MessageDecoder, serializer: Serializer) -> SpaMessage | None:
    """
    Read a response message from the given topic. Returns None if no message was received / could not be parsed.
    Creates a new connection to avoid mixing messages with the default connection.
    """
    async with client.messages() as messages:
        async for message in messages:
            message = serializer.deserialize(message_decoder(message))
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
        serializer: Serializer,
        message_decoder: MessageDecoder = _mqtt_message_decoder,
    ) -> None:
        self.config = config
        # if no message queue is given create an internal one (for later access,
        # it contains all messages which are received from the broker)
        self.message_queue = message_queue if message_queue is not None else asyncio.Queue()
        self.serializer = serializer
        self.message_decoder = message_decoder
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
                _read_messages(self.client, self.message_queue, self.message_decoder, self.serializer),
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
        await self.client.publish(message.topic, payload=self.serializer.serialize(message), qos=self.config.qos)

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
            listener = _read_response_message(client, self.message_decoder, self.serializer)

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
        serializer: Serializer,
        message_decoder: MessageDecoder = _mqtt_message_decoder,
    ) -> None:
        self.config = config
        self.message_decoder = message_decoder
        self.serializer = serializer

    def rebuild(self, topics: str | list[str] | None = None, **kwargs) -> Self:
        """
        Rebuilds the SocketProvider with a given configuration change.
        """
        new_config = copy.deepcopy(self.config)

        # normalize and set topics in config
        topics = topics or self.config.default_subscription_topics
        if topics is not None and isinstance(topics, str):
            topics = [topics]
        new_config.default_subscription_topics = topics

        return MqttSocketProvider(new_config, self.serializer, self.message_decoder)

    def create_socket(
        self,
        queue: asyncio.Queue | None,
    ) -> None:
        return MqttSocket(self.config, queue, self.serializer, self.message_decoder)
