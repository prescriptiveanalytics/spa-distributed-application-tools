import asyncio
import time
from enum import Enum
from typing import Callable, Protocol, Self

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_pascal


class SpaSocket(Protocol):
    """
    Defines the interface for SPA applications to communicate with the message bus.
    """

    async def publish():
        raise NotImplementedError()

    async def subscribe():
        raise NotImplementedError()

    async def unsubscribe():
        raise NotImplementedError()

    async def request():
        raise NotImplementedError()


class QualityOfService(Enum):
    at_most_once = 0
    at_least_once = 1
    exactly_once = 2


class SpaMessage(BaseModel):
    """
    Defines the message for SPA applications
    """

    model_config = ConfigDict(alias_generator=to_pascal)

    payload: bytes
    topic: str
    content_type: str | None = None
    client_id: str | None = None
    client_name: str | None = None
    quality_of_service: QualityOfService = 2
    response_topic: str | None = None
    timestamp: int = int(time.time())

    @staticmethod
    def build(
        topic: str,
        payload: bytes,
        *,
        response_topic: str | None = None,
        content_type: str | None = None,
        client_id: str | None = None,
        client_name: str | None = None,
        quality_of_service: QualityOfService = 2,
        timestamp: int = int(time.time()),
    ) -> Self:
        """
            Function for building a message, necessary because the constructor of pydantic models does not follow python conventions
        """
        return SpaMessage(
            Topic=topic,
            Payload=payload,
            Response_topic=response_topic,
            Content_type=content_type,
            ClientId=client_id,
            ClientName=client_name,
            QualityOfService=quality_of_service,
            Timestamp=timestamp,
        )


MessageBuilder = Callable[
    [str, bytes, str | None, str | None, str | None, str | None, QualityOfService, int], SpaMessage
]


class SocketProvider(Protocol):
    """
    A service provider is a class which creates a socket from a given configuration and returns it.
    It also allows to add a queue for communication
    """

    def rebuild(self, topics: str | list[str] | None = None, *kwargs) -> Self:
        raise NotImplementedError()

    def create_socket(self, queue: asyncio.Queue | None, topics: list[str] = None) -> SpaSocket:
        raise NotImplementedError()
