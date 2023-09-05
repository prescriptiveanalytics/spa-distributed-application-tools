import asyncio
import json
import logging
import uuid
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from spa_dat.protocol.typedef import SpaMessage, SpaSocket

logger = logging.getLogger(__name__)

MessageDecoder = Callable[[ConsumerRecord], SpaMessage]
MessageEncoder = Callable[[SpaMessage], bytes]


def _kafka_message_decoder(message: ConsumerRecord) -> SpaMessage:
    # decodes the kafka message and builds the SpaMessage from it
    try:
        message = SpaMessage(**json.loads(message.value))
        return message
    except json.JSONDecodeError as e:
        logger.error(f"Could not parse SPA message: {message.value}. Error during `json.loads`: {e}")
        return None


def _kafka_message_encoder(message: SpaMessage) -> bytes:
    """
    Encodes the SpaMessage into a kafka message.
    """
    return message.model_dump_json().encode("utf-8")


async def _read_response_message(consumer: AIOKafkaConsumer, message_decoder: MessageDecoder) -> SpaMessage | None:
    """
    Read a response message from the given topic. Returns None if no message was received / could not be parsed.
    """
    message = await consumer.getone()
    return message_decoder(message)


async def _read_messages(
    consumer: AIOKafkaConsumer, message_queue: asyncio.Queue, message_decoder: MessageDecoder
) -> None:
    async for message in consumer:
        message = message_decoder(message)
        if message is not None:
            await message_queue.put(message)


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    default_subscription_topics: str | None | list[str] = None  # if set to none no subscription will be made
    group_id: str | None = None
    client_id: str = str(uuid.uuid4())


class KafkaSocket(SpaSocket, AbstractAsyncContextManager):
    def __init__(
        self,
        config: KafkaConfig,
        message_queue: asyncio.Queue = asyncio.Queue(),
        message_decoder: MessageDecoder = _kafka_message_decoder,
        message_encoder: MessageEncoder = _kafka_message_encoder,
    ) -> None:
        self.config = config
        self.message_queue = message_queue
        self.message_decoder = message_decoder
        self.message_encoder = message_encoder

        self.consumer = AIOKafkaConsumer(**KafkaSocket._build_consumer_config(config))
        self.admin_client = AIOKafkaAdminClient(**KafkaSocket._build_producer_config(config))
        self.producer = AIOKafkaProducer(**KafkaSocket._build_producer_config(config))

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
        await self.admin_client.start()
        await self.consumer.start()
        await self.producer.start()

        # spawn tasks which reads messages
        if self.reader_task is None:
            self.reader_task = asyncio.create_task(
                _read_messages(self.consumer, self.message_queue, self.message_decoder),
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
            value=self.message_encoder(message),
        )

    async def subscribe(self, topic: str) -> None:
        self.consumer.subscribe([topic])
        logger.info(f"Subscribed to topic: {topic}")

    async def unsubscribe(self, topic: str) -> None:
        self.consumer.unsubscribe([topic])
        logger.info(f"Unsubscribed from topic: {topic}")

    def _get_ephemeral_response_topic(self) -> str:
        return f"{self.config.client_id}_request_response"

    async def request(self, message: SpaMessage) -> SpaMessage | None:
        """
        Publish a message and wait for a response. Returns none if no response was received / could not be parsed.
        """
        ephemeral_response_topic = self._get_ephemeral_response_topic()

        async with AIOKafkaConsumer(
            ephemeral_response_topic, **KafkaSocket._build_consumer_config(self.config)
        ) as consumer:
            # start listener for response
            listener = _read_response_message(consumer, self.message_decoder)

            # publish message and wait for response, set response topic
            message.response_topic = ephemeral_response_topic
            await self.publish(message)

            # wait for response
            response = await listener
        return response


class KafkaSocketProvider:
    def __init__(
        self,
        config: KafkaConfig,
        message_decoder: MessageDecoder = _kafka_message_decoder,
        message_encoder: MessageEncoder = _kafka_message_encoder,
    ) -> None:
        self.config = config
        self.message_decoder = message_decoder
        self.message_encoder = message_encoder

    def create_socket(self, queue: asyncio.Queue | None) -> KafkaSocket:
        return KafkaSocket(self.config, queue, self.message_decoder, self.message_encoder)
