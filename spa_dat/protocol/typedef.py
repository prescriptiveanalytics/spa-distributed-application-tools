import asyncio
import time
from typing import Protocol

from pydantic import BaseModel
from pydantic.dataclasses import dataclass


@dataclass
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

    client_name: str
    content_type: str
    payload: bytes
    topic: str
    response_topic: str | None = None
    quality_of_service: int = 0
    timestamp: int = int(time.time())


class SocketProvider(Protocol):
    """
    A service provider is a class which creates a socket from a given configuration and returns it.
    It also allows to add a queue for communication
    """
    def overwrite_config(self, topics: str | list[str] | None = None, *kwargs) -> None:
        raise NotImplementedError()

    def create_socket(self, queue: asyncio.Queue | None, topics: list[str] = None) -> SpaSocket:
        raise NotImplementedError()
