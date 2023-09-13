import asyncio
import copy
import logging
import uuid
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Callable, Self

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from spa_dat.socket.typedef import SocketProvider, SpaMessage, SpaSocket
from spa_dat.serialization import Serializer

logger = logging.getLogger(__name__)

MessageDecoder = Callable[[ConsumerRecord], bytes]
MessageEncoder = Callable[[SpaMessage], bytes]


def _kafka_message_decoder(message: ConsumerRecord) -> bytes:
    """
    Decodes the message value. Currently is very simple as only the value of the consumer record is used
    """
    return message.value


async def _read_response_message(
    consumer: AIOKafkaConsumer, message_decoder: MessageDecoder, serializer: Serializer
) -> SpaMessage | None:
    """
    Read a response message from the given topic. Returns None if no message was received / could not be parsed.
    """
    message = await consumer.getone()
    return serializer.deserialize(message_decoder(message))


async def _read_messages(
    consumer: AIOKafkaConsumer, message_queue: asyncio.Queue, message_decoder: MessageDecoder, serializer: Serializer
) -> None:
    async for message in consumer:
        message = serializer.deserialize(message_decoder(message))
        if message is not None:
            await message_queue.put(message)


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    default_subscription_topics: str | None | list[str] = None  # if set to none no subscription will be made
    group_id: str | None = None
    client_id: str = None


class KafkaSocket(SpaSocket, AbstractAsyncContextManager):
    def __init__(
        self,
        config: KafkaConfig,
        message_queue: asyncio.Queue | None,
        serializer: Serializer,
        message_decoder: MessageDecoder = _kafka_message_decoder,
    ) -> None:
        self.config = config
        self.serializer = serializer
        self.message_queue = message_queue
        self.message_decoder = message_decoder
        self.client_id = config.client_id or str(uuid.uuid4())

        # clients are initialized in __aenter__ - required by framework
        self.consumer = None
        self.admin_client = None
        self.producer = None

        self.reader_task = None

        # these are created by the client and deleted again
        self.managed_topics = set()
        if config.default_subscription_topics is not None:
            if isinstance(config.default_subscription_topics, str):
                self.managed_topics.update([config.default_subscription_topics])
            else:
                # we expect it to be a list or list like structure
                self.managed_topics.update(config.default_subscription_topics)

        # add topic for response messages
        self.managed_topics.add(self._get_ephemeral_response_topic())

    @staticmethod
    def _build_consumer_config(config: KafkaConfig) -> dict:
        return {
            "bootstrap_servers": config.bootstrap_servers,
            "group_id": config.group_id,
            "client_id": config.client_id,
        }

    @staticmethod
    def _build_producer_config(config) -> dict:
        return {
            "bootstrap_servers": config.bootstrap_servers,
            "client_id": config.client_id,
        }

    async def __aenter__(self):
        """Return `self` upon entering the runtime context."""
        if None in [self.admin_client, self.consumer, self.producer]:
            self.admin_client = AIOKafkaAdminClient(**KafkaSocket._build_producer_config(self.config))
            self.consumer = AIOKafkaConsumer(**KafkaSocket._build_consumer_config(self.config))
            self.producer = AIOKafkaProducer(**KafkaSocket._build_producer_config(self.config))

        await self.admin_client.start()
        await self.consumer.start()
        await self.producer.start()

        # spawn tasks which reads messages
        if self.reader_task is None:
            self.reader_task = asyncio.create_task(
                _read_messages(self.consumer, self.message_queue, self.message_decoder, self.serializer),
                name="task-kafka-reader",
            )

        # subscribe to default topic
        if self.managed_topics is not None:
            await self.admin_client.create_topics(
                [NewTopic(name=name, num_partitions=1, replication_factor=1) for name in self.managed_topics],
            )
        if self.config.default_subscription_topics is not None:
            await self.subscribe(self.config.default_subscription_topics)

        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Raise any exception triggered within the runtime context."""
        if self.reader_task is not None:
            self.reader_task.cancel()
            self.reader_task = None

        if self.managed_topics is not None:
            await self.admin_client.delete_topics(list(self.managed_topics))

        await self.consumer.stop()
        await self.producer.stop()
        await self.admin_client.close()
        return None

    async def publish(self, message: SpaMessage) -> None:
        await self.producer.send_and_wait(
            topic=message.topic,
            value=self.serializer.serialize(message),
        )

    async def subscribe(self, topic: list[str]) -> None:
        self.consumer.subscribe(topic)
        logger.info(f"Subscribed to topic: {topic}")

    async def unsubscribe(self, topic: list[str]) -> None:
        self.consumer.unsubscribe(topic)
        logger.info(f"Unsubscribed from topic: {topic}")

    def _get_ephemeral_response_topic(self) -> str:
        return f"{self.client_id}_request_response"

    async def request(self, message: SpaMessage) -> SpaMessage | None:
        """
        Publish a message and wait for a response. Returns none if no response was received / could not be parsed.
        """
        ephemeral_response_topic = self._get_ephemeral_response_topic()

        async with AIOKafkaConsumer(
            ephemeral_response_topic, **KafkaSocket._build_consumer_config(self.config)
        ) as consumer:
            # start listener for response
            listener = _read_response_message(consumer, self.message_decoder, self.serializer)

            # publish message and wait for response, set response topic
            message.response_topic = ephemeral_response_topic
            await self.publish(message)

            # wait for response
            response = await listener

        # return deserialized message
        return response


class KafkaSocketProvider(SocketProvider):
    def __init__(
        self,
        config: KafkaConfig,
        serializer: Serializer,
        message_decoder: MessageDecoder = _kafka_message_decoder,
    ) -> None:
        self.config = config
        self.serializer = serializer
        self.message_decoder = message_decoder

    def rebuild(self, topics: str | list[str] | None = None, *kwargs) -> Self:
        """
        Rebuilds the SocketProvider with a given configuration change.
        """
        new_config = copy.deepcopy(self.config)

        # normalize and set topics in config
        topics = topics or self.config.default_subscription_topics
        if topics is not None and isinstance(topics, str):
            topics = [topics]
        new_config.default_subscription_topics = topics

        return KafkaSocketProvider(new_config, self.serializer, self.message_decoder)

    def create_socket(self, queue: asyncio.Queue | None) -> KafkaSocket:
        return KafkaSocket(self.config, queue, self.serializer, self.message_decoder)
