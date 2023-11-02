import asyncio
import time
from typing import Protocol, Self

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


class SpaMessage(BaseModel):
    """
    Defines the message for SPA applications
    """
    model_config = ConfigDict(alias_generator=to_pascal)

    payload: bytes
    topic: str
    # content type of the payload
    content_type: str | None = None
    # uuid of the client
    client_id: str | None = None
    # name of the client
    client_name: str | None = None
    # enum
    # 0: at most once
    # 1: at least once
    # 2: exactly once
    quality_of_service: int = 2
    response_topic: str | None = None
    timestamp: int = int(time.time())



class SocketProvider(Protocol):
    """
    A service provider is a class which creates a socket from a given configuration and returns it.
    It also allows to add a queue for communication
    """

    def rebuild(self, topics: str | list[str] | None = None, *kwargs) -> Self:
        raise NotImplementedError()

    def create_socket(self, queue: asyncio.Queue | None, topics: list[str] = None) -> SpaSocket:
        raise NotImplementedError()
