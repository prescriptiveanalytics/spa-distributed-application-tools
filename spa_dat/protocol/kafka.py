import asyncio
import json
import logging
import uuid
from contextlib import AbstractAsyncContextManager
from typing import Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.helpers import create_ssl_context

from spa_dat.protocol.typedef import SpaMessage, SpaSocket
from spa_dat.provider import KafkaConfig

logger = logging.getLogger(__name__)

MessageDecoder = Callable[[bytes], SpaMessage]
MessageEncoder = Callable[[SpaMessage], bytes]


def _kafka_message_decoder(message: bytes) -> SpaMessage:
    # decodes the kafka message and builds the SpaMessage from it
    try:
        message = SpaMessage(**json.loads(message.decode()))
        return message
    except json.JSONDecodeError as e:
        logger.error(f"Could not parse SPA message: {message.decode()}. Error during `json.loads`: {e}")


def _kafka_message_encoder(message: SpaMessage) -> bytes:
    """
    Encodes the SpaMessage into a kafka message.
    """
    return message.model_dump_json().encode("utf-8")


class KafkaService(SpaSocket, AbstractAsyncContextManager):
    def __init__(
        self,
        config: KafkaConfig,
        message_queue: asyncio.Queue = asyncio.Queue(),
        message_decoder: MessageDecoder = _kafka_message_decoder,
        message_encoder: MessageEncoder = _kafka_message_encoder,
    ) -> None:
        self.kafka_config = config
        self.message_queue = message_queue
        self.message_decoder = message_decoder
        self.message_encoder = message_encoder
        self._consumer_config = self.build_consumer_config()
        self._producer_config = self.build_producer_config()
        self.consumer = AIOKafkaConsumer(**self._consumer_config)
        self.producer = AIOKafkaProducer(**self._producer_config)
        self.reader_task = None

    def build_consumer_config(self) -> dict:
        """
        Build a consumer config for the Kafka consumer.
        """
        ssl_context = create_ssl_context(
            cafile=self.kafka_config.ca_file,
            certfile=self.kafka_config.cert_file,
            keyfile=self.kafka_config.key_file,
            password=self.kafka_config.key_password,
        )

        return dict(
            bootstrap_servers=self.kafka_config.bootstrap_servers,
            group_id=self.kafka_config.group_id,
            security_protocol=self.kafka_config.security_protocol,
            ssl_context=ssl_context,
        )

    def build_producer_config(self) -> dict:
        """
        Build a producer config for the Kafka producer.
        """
        ssl_context = create_ssl_context(
            cafile=self.kafka_config.ca_file,
            certfile=self.kafka_config.cert_file,
            keyfile=self.kafka_config.key_file,
            password=self.kafka_config.key_password,
        )

        return dict(
            bootstrap_servers=self.kafka_config.bootstrap_servers,
            security_protocol=self.kafka_config.security_protocol,
            ssl_context=ssl_context,
        )

    async def __aenter__(self):
        """Return `self` upon entering the runtime context."""
        await self.consumer.start()
        await self.producer.start()

        # spawn tasks which reads messages
        self.reader_task = asyncio.create_task(
            self._read_messages(),
            name="task-kafka-reader",
        )

        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Raise any exception triggered within the runtime context."""
        if self.reader_task is not None:
            self.reader_task.cancel()
            self.reader_task = None

        await self.consumer.stop()
        await self.producer.stop()
        return None

    async def publish(self, message: SpaMessage) -> None:
        await self.producer.send_and_wait(
            topic=message.topic,
            value=self.message_encoder(message),
        )

    async def subscribe(self, topic: str) -> None:
        await self.consumer.subscribe([topic])
        logger.info(f"Subscribed to topic: {topic}")

    async def unsubscribe(self, topic: str) -> None:
        await self.consumer.unsubscribe([topic])
        logger.info(f"Unsubscribed from topic: {topic}")

    def _get_ephemeral_response_topic(self, topic: str) -> str:
        return f"{topic}/request/{uuid.uuid4()}"

    async def request(self, message: SpaMessage) -> SpaMessage | None:
        """
        Publish a message and wait for a response. Returns none if no response was received / could not be parsed.
        """
        ephemeral_response_topic = self._get_ephemeral_response_topic(message.topic)

        async with AIOKafkaConsumer(
            bootstrap_servers=self.kafka_config.bootstrap_servers,
            security_protocol=self.kafka_config.security_protocol,
            ssl_context=create_ssl_context(
                cafile=self.kafka_config.ca_file,
                certfile=self.kafka_config.cert_file,
                keyfile=self.kafka_config.key_file,
                password=self.kafka_config.key_password,
            ),
        ) as consumer:
            await consumer.subscribe([ephemeral_response_topic])

            # start listener for response
            listener = self._read_response_message(consumer)

            # publish message and wait for response, set response topic
            message.response_topic = ephemeral_response_topic
            await self.publish(message)

            # wait for response
            response = await listener
            await consumer.unsubscribe([ephemeral_response_topic])
        return response

    async def _read_response_message(self, consumer: AIOKafkaConsumer) -> SpaMessage | None:
        """
        Read a response message from the given topic. Returns None if no message was received / could not be parsed.
        """
        async for message in consumer:
            message = self.message_decoder(message.value)
            return message

    async def _read_messages(self):
        async for message in self.consumer:
            message = self.message_decoder(message.value)
            if message is not None:
                await self.message_queue.put(message)
